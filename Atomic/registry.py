import oauthlib
import http.client
import re
import urllib.parse
import urllib.request
import json
import os
import shutil
import concurrent.futures
import tempfile

class Registry(object):

    def __init__(self, registry):
        self._registry = registry
        self._connection = None
        self._token = None

    def connect(self):
        self._connection = http.client.HTTPSConnection(self._registry)

    def _request_token(self, authline):
        reg = re.compile('(\w+)= *"?([^"]+)"?')
        auth = dict(reg.findall(authline))
        realm = urllib.parse.urlparse(auth["realm"])
        connection = http.client.HTTPSConnection(realm.netloc)
        url = "%s?service=%s&scope=%s" % (realm.path, auth["service"], auth["scope"])
        connection.request("GET", url)
        r = connection.getresponse()
        self._token = json.loads(r.readall().decode())["token"]
        return self._token

    def _do_request(self, connection, method, url, retry=True):
        headers = {}
        if self._token:
            headers["Authorization"] = "Bearer %s" % self._token
        connection.request(method, url, headers=headers)
        r = connection.getresponse()
        if int(r.getcode() / 100) == 3:
            location = dict(r.getheaders())["Location"]
            return urllib.request.urlopen(location)
        elif r.getcode() == 401:
            for i in r.getheaders():
                if retry and i[0] == "Www-Authenticate" and "Bearer" in i[1]:
                    r.close()
                    self._request_token(i[1])
                    return self._do_request(connection, method, url, retry=False)
        return r

    def manifest(self, image, tag):
        r = self._do_request(self._connection, "GET", "/v2/%s/manifests/%s" % (image, tag))
        if r.getcode() == 200:
            return r.readall().decode()
        return None

    def _do_fetch_layer(self, connection, image, blob, filename):
        r = self._do_request(connection, "GET", "/v2/%s/blobs/%s" % (image, blob))
        if r.getcode() == 200:
            with open(filename, "wb") as f:
                while not r.closed:
                    data = r.read(4096)
                    if (len(data)) == 0:
                        break
                    f.write(data)

    def fetch_layer(self, image, blob, filename):
        return self._do_fetch_layer(self._connection, image, blob, filename)

    def layers(self, image, tag, manifest=None):
        if not manifest:
            manifest = self.manifest(image, tag)
            if not manifest:
                return None
        manifest_json = json.loads(manifest)
        layers = list(i["blobSum"] for i in manifest_json["fsLayers"])
        layers.reverse()
        return layers

    def fetch_layers(self, image, layers):
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            def worker_fetch_layer(image, layer):
                connection = http.client.HTTPSConnection(self._registry)
                destination_file = tempfile.NamedTemporaryFile("wb")
                self._do_fetch_layer(connection, image, layer, destination_file.name)
                return (layer, destination_file)

            futures = {executor.submit(worker_fetch_layer, image, layer) : layer for layer in layers}
        ret = {}
        for future in concurrent.futures.as_completed(futures):
            (layer, destination_file) = future.result()
            ret[layer] = destination_file
        return ret
