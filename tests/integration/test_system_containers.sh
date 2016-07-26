#!/bin/bash -x
set -euo pipefail
IFS=$'\n\t'

# Test scripts run with PWD=tests/..

# The test harness exports some variables into the environment during
# testing: PYTHONPATH (python module import path
#          WORK_DIR   (a directory that is safe to modify)
#          DOCKER     (the docker executable location)
#          ATOMIC     (an invocation of 'atomic' which measures code coverage)
#          SECRET     (a generated sha256 hash inserted into test containers)

# In addition, the test harness creates some images for use in testing.
#   See tests/test-images/

OUTPUT=$(/bin/true)

# Skip the test if OSTree or runc are not installed, or atomic has not --install --system
ostree --version &>/dev/null || exit 77
runc --version &>/dev/null || exit 77
${ATOMIC}  install --help 2>&1 | grep -q -- --system || exit 77

export ATOMIC_OSTREE_REPO=${WORK_DIR}/repo
export ATOMIC_OSTREE_CHECKOUT_PATH=${WORK_DIR}/checkout

docker save atomic-test-system > ${WORK_DIR}/atomic-test-system.tar

${ATOMIC} pull dockertar:/${WORK_DIR}/atomic-test-system.tar

${ATOMIC} pull docker.io/busybox

# Check that the branch is created in the OSTree repository
ostree --repo=${ATOMIC_OSTREE_REPO} refs | grep -q "ociimage/atomic-test-system-latest"

${ATOMIC} images list > ${WORK_DIR}/images
grep -q "atomic-test-system" ${WORK_DIR}/images

export NAME="test-system-container-$$"

teardown () {
    set +o pipefail

    # Do not leave the runc container in any case
    runc kill $NAME 9 &> /dev/null || true
    runc delete $NAME &> /dev/null  || true

    # Ensure there is no systemd service left running
    systemctl stop $NAME &> /dev/null || true
    systemctl disable $NAME &> /dev/null || true
    rm -rf /etc/systemd/system/${NAME}.service || true
}

trap teardown EXIT

${ATOMIC} --debug install --name=${NAME} --set=RECEIVER=${SECRET} --system oci:atomic-test-system

${ATOMIC} ps | grep -q "test-system"
${ATOMIC} ps --json | grep -q "test-system"
${ATOMIC} ps --all | grep -q "test-system"
${ATOMIC} ps --json --all | grep -q "test-system"
${ATOMIC} ps --filter id=test-system | grep -q "test-system"
${ATOMIC} ps --no-trunc | grep -q "test-system"
${ATOMIC} ps --quiet | grep -q "test-system"
${ATOMIC} ps -aq --no-trunc --filter id=test-system | grep -q "test-system"
if ${ATOMIC} ps -aq --no-trunc --filter id=non-existing-system | grep -q "test-system"; then
    exit 1
fi

test -e /etc/systemd/system/${NAME}.service

# The value we set is exported into the config file
grep -q ${SECRET} ${ATOMIC_OSTREE_CHECKOUT_PATH}/${NAME}.0/config.json

# The default value $PORT specified in the manifest.json is exported
grep -q 8081 ${ATOMIC_OSTREE_CHECKOUT_PATH}/${NAME}.0/config.json

${ATOMIC} update --set=PORT=8082 --container ${NAME}

# Check that the same SECRET value is kept, and that $PORT gets the new value
grep -q ${SECRET} ${ATOMIC_OSTREE_CHECKOUT_PATH}/${NAME}.1/config.json
grep -q 8082 ${ATOMIC_OSTREE_CHECKOUT_PATH}/${NAME}.1/config.json

mkdir ${WORK_DIR}/mount

# Test that mount and umount work

# Check that --live and --shared fail
OUTPUT=$(! ${ATOMIC} mount --live ${NAME} ${WORK_DIR}/mount 2>&1)
grep "do not support --live" <<< $OUTPUT

OUTPUT=$(! ${ATOMIC} mount --shared ${NAME} ${WORK_DIR}/mount 2>&1)
grep "do not support --shared" <<< $OUTPUT

# mount a container
${ATOMIC} mount ${NAME} ${WORK_DIR}/mount
test -e ${WORK_DIR}/mount/usr/bin/greet.sh
${ATOMIC} umount ${WORK_DIR}/mount

# mount an image
${ATOMIC} mount atomic-test-system ${WORK_DIR}/mount
test -e ${WORK_DIR}/mount/usr/bin/greet.sh
${ATOMIC} umount ${WORK_DIR}/mount

${ATOMIC} uninstall ${NAME}
test \! -e /etc/systemd/system/${NAME}.service
test \! -e ${ATOMIC_OSTREE_CHECKOUT_PATH}/${NAME}
test \! -e ${ATOMIC_OSTREE_CHECKOUT_PATH}/${NAME}.0
test \! -e ${ATOMIC_OSTREE_CHECKOUT_PATH}/${NAME}.1

# check that there are not any "ociimage/" prefixed branch left after images prune
${ATOMIC} images delete -f atomic-test-system
${ATOMIC} images delete -f busybox
${ATOMIC} images prune
OUTPUT=$(! ostree --repo=${ATOMIC_OSTREE_REPO} refs | grep -c ociimage)
if test $OUTPUT \!= 0; then
    exit 1
fi
