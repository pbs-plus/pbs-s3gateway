package s3

import (
	"encoding/xml"
	"io"
	"time"
)

// S3Object represents an object in S3 ListBucketResult.
type S3Object struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag,omitempty"`
	Size         int64     `xml:"Size"`
	StorageClass string    `xml:"StorageClass,omitempty"`
}

// ListBucketResult is the S3 XML response for listing objects.
type ListBucketResult struct {
	XMLName     xml.Name   `xml:"ListBucketResult"`
	Xmlns       string     `xml:"xmlns,attr"`
	Name        string     `xml:"Name"`
	Prefix      string     `xml:"Prefix"`
	Marker      string     `xml:"Marker,omitempty"`
	MaxKeys     int        `xml:"MaxKeys,omitempty"`
	IsTruncated bool       `xml:"IsTruncated"`
	Contents    []S3Object `xml:"Contents,omitempty"`
}

// S3Error is the S3 XML error response.
type S3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

// Bucket represents an S3 bucket in ListAllMyBucketsResult.
type Bucket struct {
	Name string `xml:"Name"`
}

// ListAllMyBucketsResult is the S3 XML response for listing buckets.
type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   string   `xml:"Owner>DisplayName"`
	Buckets []Bucket `xml:"Buckets>Bucket"`
}

// WriteListResponse writes an S3 ListBucketResult XML response directly to w.
func WriteListResponse(w io.Writer, bucket, prefix, marker string, objects []S3Object) {
	result := ListBucketResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:        bucket,
		Prefix:      prefix,
		Marker:      marker,
		MaxKeys:     1000,
		IsTruncated: false,
		Contents:    objects,
	}
	xml.NewEncoder(w).Encode(result)
}

// WriteListBucketsResponse writes an S3 ListAllMyBucketsResult XML response directly to w.
func WriteListBucketsResponse(w io.Writer, buckets []string) {
	result := ListAllMyBucketsResult{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: "s3gateway",
	}
	for _, name := range buckets {
		result.Buckets = append(result.Buckets, Bucket{Name: name})
	}
	xml.NewEncoder(w).Encode(result)
}

// BuildListResponse constructs an S3 ListBucketResult XML response as bytes.
func BuildListResponse(bucket, prefix, marker string, objects []S3Object) ([]byte, error) {
	result := ListBucketResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:        bucket,
		Prefix:      prefix,
		Marker:      marker,
		MaxKeys:     1000,
		IsTruncated: false,
		Contents:    objects,
	}
	return xml.Marshal(result)
}

// BuildListBucketsResponse constructs an S3 ListAllMyBucketsResult XML response as bytes.
func BuildListBucketsResponse(buckets []string) ([]byte, error) {
	result := ListAllMyBucketsResult{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: "s3gateway",
	}
	for _, name := range buckets {
		result.Buckets = append(result.Buckets, Bucket{Name: name})
	}
	return xml.Marshal(result)
}

// BuildErrorResponse constructs an S3 error XML response.
func BuildErrorResponse(code, message, resource, requestID string) []byte {
	err := S3Error{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestID: requestID,
	}
	data, _ := xml.Marshal(err)
	return data
}
