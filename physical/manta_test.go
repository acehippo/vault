package physical

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/hashicorp/vault/helper/logformat"
	"github.com/joyent/gocommon/client"
	"github.com/joyent/gomanta/manta"
	"github.com/joyent/gosign/auth"
	log "github.com/mgutz/logxi/v1"
)

func TestMantaBackend(t *testing.T) {
	endpoint := os.Getenv("MANTA_URL")
	user := os.Getenv("MANTA_USER")
	keyid := os.Getenv("MANTA_KEY_ID")
	if endpoint == "" || user == "" || keyid == "" {
		t.SkipNow()
	}

	privateKeyPath := os.Getenv("HOME") + "/.ssh/id_rsa"
	privateKey, err := ioutil.ReadFile(privateKeyPath)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	authentication, err := auth.NewAuth(user, string(privateKey), "rsa-sha256")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	credsConfig := &auth.Credentials{
		UserAuthentication: authentication,
		MantaKeyId:         keyid,
		MantaEndpoint:      auth.Endpoint{URL: endpoint},
	}

	client := client.NewClient(credsConfig.MantaEndpoint.URL, "", credsConfig, nil)
	mantaClient := manta.New(client)

	var randInt = rand.New(rand.NewSource(time.Now().UnixNano())).Int()
	bucket := fmt.Sprintf("vault-manta-testacc-%d", randInt)

	err = mantaClient.PutDirectory(bucket)
	if err != nil {
		t.Fatalf("unable to create test bucket: %s", err)
	}

	defer func() {
		// Gotta list all the objects and delete them
		// before being able to delete the bucket
		deleteObjects(mantaClient, bucket)

		err := mantaClient.DeleteDirectory(bucket)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
	}()

	logger := logformat.NewVaultLogger(log.LevelTrace)

	b, err := NewBackend("manta", logger, map[string]string{
		"endpoint": credsConfig.MantaEndpoint.URL,
		"keyid":    credsConfig.MantaKeyId,
		"user":     user,
		"path":     bucket,
	})
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	testBackend(t, b)
	testBackend_ListPrefix(t, b)
}

func deleteObjects(client *manta.Client, dir string) {

	listResp, _ := client.ListDirectory(dir, manta.ListDirectoryOpts{})
	for _, item := range listResp {
		if item.Type == "directory" {
			subDir := path.Join(dir, item.Name)
			deleteObjects(client, subDir)
			client.DeleteDirectory(subDir)
		} else {
			client.DeleteObject(dir, item.Name)
		}
	}
}
