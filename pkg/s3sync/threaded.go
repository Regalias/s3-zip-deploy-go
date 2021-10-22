package s3sync

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func (h *SyncHandler) uploadFile(ctx context.Context, uploader *manager.Uploader, fileName string) error {
	// Guess the mime type and encoding
	contentType, contentEncoding := guessFileMetadata(fileName)

	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed opening file '%s': %v", fileName, err)
	}
	defer file.Close()

	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:          &h.targetBucket,
		Key:             aws.String(fileName),
		Body:            file,
		ContentType:     &contentType,
		ContentEncoding: contentEncoding,
	})
	if err != nil {
		return fmt.Errorf("failed to upload '%s': %v", fileName, err)
	}
	log.Printf("Uploaded %s (%s)\n", fileName, getMetadataString(contentType, contentEncoding))
	return nil
}

func (h *SyncHandler) UploadFileListThreaded(ctx context.Context, fileList []string) error {
	log.Printf("Upload %d files -> '%s'", len(fileList), h.targetBucket)

	eg, ctx := errgroup.WithContext(ctx)
	uploader := manager.NewUploader(h.s3Client)

	sem := semaphore.NewWeighted(MAX_ROUTINES)

	eg.Go(func() error {
		for _, fileName := range fileList {
			// if ctx.Err() != nil {
			// 	break
			// }

			if err := sem.Acquire(ctx, 1); err != nil {
				return err
			}

			fname := fileName
			// i := i
			eg.Go(func() error {
				defer sem.Release(1)
				err := h.uploadFile(ctx, uploader, fname)
				// log.Printf("Routine %d done\n", i)
				return err
			})
		}
		return nil
	})

	return eg.Wait()
}
