package main
import (
	"fmt"
	"compress/gzip"
	"os"
	"io"
	"crypto/sha256"
)

func main() {
	reader, err := os.Open(os.Args[1])
	if err != nil {
		os.Exit(1)
	}
	buf := make([]byte, 4096)
	sha_256 := sha256.New()
	w := gzip.NewWriter(sha_256)

	for true {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n <= 0 {
			break
		}
		w.Write(buf[:n])
	}
	w.Close()

	fmt.Printf("%x\n", sha_256.Sum(nil))
}
