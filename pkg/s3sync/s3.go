package s3sync

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type SyncHandler struct {
	s3Client     *s3.Client
	targetBucket string
	sourceBucket string
	sourceKey    string
}

func New(cfg aws.Config, targetBucket string, sourceBucket string, sourceKey string) *SyncHandler {
	return &SyncHandler{
		s3Client:     s3.NewFromConfig(cfg),
		targetBucket: targetBucket,
		sourceBucket: sourceBucket,
		sourceKey:    sourceKey,
	}
}

func (h *SyncHandler) FetchObjectListFromTarget(ctx context.Context) ([]types.Object, error) {

	var contents []types.Object

	// List objects and depaginate
	paginator := s3.NewListObjectsV2Paginator(h.s3Client, &s3.ListObjectsV2Input{
		Bucket: &h.targetBucket,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("ListObjectsV2 on '%s' failed: %v", h.targetBucket, err)
		}
		contents = append(contents, page.Contents...)
	}

	return contents, nil
}

func (h *SyncHandler) DownloadSourceBundle(ctx context.Context, downloadTargetPath string) error {

	downloader := manager.NewDownloader(h.s3Client, func(d *manager.Downloader) {
		d.Concurrency = 2
		d.PartSize = 5 * 1024 * 1024 // 5 MB
	})

	fd, err := os.Create(downloadTargetPath)
	if err != nil {
		return err
	}
	defer fd.Close()

	numBytes, err := downloader.Download(context.TODO(), fd, &s3.GetObjectInput{
		Bucket: aws.String(h.sourceBucket),
		Key:    aws.String(h.sourceKey),
	})
	if err != nil {
		return fmt.Errorf("failed to download file '%s/%s': %v", h.sourceBucket, h.sourceKey, err)
	}
	log.Printf("Downloaded '%s/%s' -> %s (%d bytes)\n", h.sourceBucket, h.sourceKey, downloadTargetPath, numBytes)

	return nil
}

func (h *SyncHandler) UploadFileList(ctx context.Context, fileList []string) error {
	log.Printf("Upload %d files -> '%s'", len(fileList), h.targetBucket)

	uploader := manager.NewUploader(h.s3Client, func(u *manager.Uploader) {
		u.Concurrency = 3
		u.PartSize = 5 * 1024 * 1024 // 5mb
	})

	for _, fileName := range fileList {

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
	}

	return nil
}

func (h *SyncHandler) DeleteObjects(ctx context.Context, filesToDelete []string) error {
	log.Printf("Delete %d object(s): %v\n", len(filesToDelete), filesToDelete)
	var objectsToDelete = make([]types.ObjectIdentifier, len(filesToDelete))
	for i, file := range filesToDelete {
		objectsToDelete[i] = types.ObjectIdentifier{Key: aws.String(file)}
	}
	return h.deleteObjects(ctx, objectsToDelete)
}

func (h *SyncHandler) EmptyBucket(ctx context.Context) error {
	objects, err := h.FetchObjectListFromTarget(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch objects from bucket: %v", err)
	}
	log.Printf("Emptying bucket: deleting %d object(s)\n", len(objects))
	var objectsToDelete = make([]types.ObjectIdentifier, len(objects))
	for i, obj := range objects {
		objectsToDelete[i] = types.ObjectIdentifier{Key: obj.Key}
	}
	return h.deleteObjects(ctx, objectsToDelete)
}

func (h *SyncHandler) deleteObjects(ctx context.Context, objectsToDelete []types.ObjectIdentifier) error {
	for i := 0; i < len(objectsToDelete); i += MAX_KEYS {
		end := min(i+MAX_KEYS, len(objectsToDelete))

		_, err := h.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &h.targetBucket,
			Delete: &types.Delete{
				Objects: objectsToDelete[i:end],
			},
		})
		if err != nil {
			return fmt.Errorf("DeleteObjects call failed: %v", err)
		}
	}
	return nil
}
