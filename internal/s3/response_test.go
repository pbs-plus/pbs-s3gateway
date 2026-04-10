package s3

import (
	"encoding/xml"
	"strings"
	"testing"
	"time"
)

func TestListBucketResultXML(t *testing.T) {
	resp := ListBucketResult{
		Name:   "my-bucket",
		Prefix: "backups/",
		Contents: []S3Object{
			{Key: "backups/a.sql", Size: 1024, LastModified: mustParseTime("2024-01-15T10:30:00Z")},
			{Key: "backups/b.sql", Size: 2048, LastModified: mustParseTime("2024-01-16T10:30:00Z")},
		},
		IsTruncated: false,
	}

	data, err := xml.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}

	raw := string(data)
	if !strings.Contains(raw, "<Name>my-bucket</Name>") {
		t.Errorf("missing Name in XML: %s", raw)
	}
	if !strings.Contains(raw, "<Key>backups/a.sql</Key>") {
		t.Errorf("missing first object key: %s", raw)
	}
	if !strings.Contains(raw, "<IsTruncated>false</IsTruncated>") {
		t.Errorf("missing IsTruncated: %s", raw)
	}
}

func TestListBucketResultEmpty(t *testing.T) {
	resp := ListBucketResult{
		Name:        "my-bucket",
		Prefix:      "backups/",
		Contents:    []S3Object{},
		IsTruncated: false,
	}

	data, err := xml.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}

	raw := string(data)
	if strings.Contains(raw, "<Contents>") {
		t.Errorf("empty result should not have Contents: %s", raw)
	}
}

func TestS3ErrorResponseXML(t *testing.T) {
	resp := S3Error{
		Code:      "NoSuchKey",
		Message:   "The specified key does not exist.",
		Resource:  "/my-bucket/backups/missing.sql",
		RequestID: "req-123",
	}

	data, err := xml.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}

	raw := string(data)
	if !strings.Contains(raw, "<Code>NoSuchKey</Code>") {
		t.Errorf("missing error code: %s", raw)
	}
	if !strings.Contains(raw, "<Message>The specified key does not exist.</Message>") {
		t.Errorf("missing error message: %s", raw)
	}
}

func TestListBucketResultWithMarker(t *testing.T) {
	resp := ListBucketResult{
		Name:        "my-bucket",
		Prefix:      "",
		Marker:      "backups/a.sql",
		Contents:    []S3Object{},
		IsTruncated: false,
	}

	data, err := xml.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}

	raw := string(data)
	if !strings.Contains(raw, "<Marker>backups/a.sql</Marker>") {
		t.Errorf("missing Marker: %s", raw)
	}
}

func TestS3ObjectLastModifiedFormat(t *testing.T) {
	obj := S3Object{
		Key:          "test.txt",
		LastModified: mustParseTime("2024-01-15T10:30:00Z"),
		Size:         100,
	}

	data, err := xml.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}

	raw := string(data)
	if !strings.Contains(raw, "2024-01-15") {
		t.Errorf("missing date in LastModified: %s", raw)
	}
}

func TestBuildListResponse(t *testing.T) {
	objects := []S3Object{
		{Key: "a.txt", Size: 10, LastModified: mustParseTime("2024-01-15T10:30:00Z")},
		{Key: "b.txt", Size: 20, LastModified: mustParseTime("2024-01-16T10:30:00Z")},
	}

	xmlBytes, err := BuildListResponse("test-bucket", "prefix/", "", objects)
	if err != nil {
		t.Fatal(err)
	}

	raw := string(xmlBytes)
	if !strings.Contains(raw, `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`) {
		t.Errorf("missing S3 namespace: %s", raw)
	}
	if !strings.Contains(raw, "<Key>a.txt</Key>") {
		t.Errorf("missing object: %s", raw)
	}
}

func TestBuildListBucketsResponse(t *testing.T) {
	buckets := []string{"team-a", "team-b"}
	xmlBytes, err := BuildListBucketsResponse(buckets)
	if err != nil {
		t.Fatal(err)
	}

	raw := string(xmlBytes)
	if !strings.Contains(raw, "<DisplayName>s3gateway</DisplayName>") {
		t.Errorf("missing owner: %s", raw)
	}
	if !strings.Contains(raw, "<Name>team-a</Name>") {
		t.Errorf("missing bucket team-a: %s", raw)
	}
	if !strings.Contains(raw, "<Name>team-b</Name>") {
		t.Errorf("missing bucket team-b: %s", raw)
	}
}

func TestBuildListBucketsResponseEmpty(t *testing.T) {
	xmlBytes, err := BuildListBucketsResponse(nil)
	if err != nil {
		t.Fatal(err)
	}

	raw := string(xmlBytes)
	if strings.Contains(raw, "<Bucket>") {
		t.Errorf("empty response should not have Bucket elements: %s", raw)
	}
}

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
