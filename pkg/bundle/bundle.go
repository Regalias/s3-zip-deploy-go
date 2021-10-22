package bundle

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type ArchiveManifest map[string]time.Time

func DecompressBundle(bundleName string, destinationPath string) (ArchiveManifest, error) {
	archive, err := zip.OpenReader(bundleName)
	if err != nil {
		return nil, fmt.Errorf("opening zip file '%s' failed: %v", bundleName, err)
	}
	defer archive.Close()

	// var fileListing []*FileDetail

	fileList := ArchiveManifest{}

	for _, f := range archive.File {
		path := filepath.Join(destinationPath, f.Name)

		if f.FileInfo().IsDir() {
			os.MkdirAll(path, os.ModePerm)
			continue
		}

		if err := os.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
			return nil, fmt.Errorf("mkdir failed: %v", err)
		}

		dstFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return nil, fmt.Errorf("open dest file failed: %v", err)
		}

		fileInArchive, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("opening source file failed: %v", err)
		}

		if _, err := io.Copy(dstFile, fileInArchive); err != nil {
			return nil, fmt.Errorf("copy file failed: %v", err)
		}
		os.Chtimes(path, time.Now(), f.Modified)
		fileList[f.Name] = f.Modified

		fileInArchive.Close()
		dstFile.Close()
	}

	return fileList, nil
}
