package physical

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/armon/go-metrics"
	"github.com/joyent/gocommon/client"
	"github.com/joyent/gomanta/manta"
	"github.com/joyent/gosign/auth"
	log "github.com/mgutz/logxi/v1"
)

// MantaBackend is a physical backend that stores data
// within an Manta client.
type MantaBackend struct {
	bucket string
	client *manta.Client
	logger log.Logger
}

// newMantaBackend constructs a Manta backend using a pre-existing
// directory. Credentials can be provided to the backend, sourced
// from the environment.
func newMantaBackend(conf map[string]string, logger log.Logger) (Backend, error) {
	endpoint := os.Getenv("MANTA_URL")
	if endpoint == "" {
		endpoint = conf["endpoint"]
		if endpoint == "" {
			return nil, fmt.Errorf("'endpoint' must be set")
		}
	}

	user := os.Getenv("MANTA_USER")
	if user == "" {
		user = conf["user"]
		if user == "" {
			return nil, fmt.Errorf("'user' must be set")
		}
	}

	keyid := os.Getenv("MANTA_KEY_ID")
	if keyid == "" {
		keyid = conf["keyid"]
		if keyid == "" {
			return nil, fmt.Errorf("'keyid' must be set")
		}
	}

	path, ok := conf["path"]
	if !ok {
		return nil, fmt.Errorf("'path' must be set")
	}

	privateKeyPath, ok := conf["keypath"]
	if !ok {
		privateKeyPath = os.Getenv("HOME") + "/.ssh/id_rsa"
	}

	privateKey, err := ioutil.ReadFile(privateKeyPath)
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	authentication, err := auth.NewAuth(user, string(privateKey), "rsa-sha256")
	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	credsConfig := &auth.Credentials{
		UserAuthentication: authentication,
		MantaKeyId:         keyid,
		MantaEndpoint:      auth.Endpoint{URL: endpoint},
	}

	client := client.NewClient(credsConfig.MantaEndpoint.URL, "", credsConfig, nil)
	mantaClient := manta.New(client)
	if mantaClient == nil {
		return nil, fmt.Errorf("fail to create Manta client")
	}

	m := &MantaBackend{
		bucket: path,
		client: mantaClient,
		logger: logger,
	}
	return m, nil
}

// Put is used to insert or update an entry
func (m *MantaBackend) Put(entry *Entry) error {
	defer metrics.MeasureSince([]string{"manta", "put"}, time.Now())

	dir, objectName := m.getPathAndObjectName(entry.Key)
	m.makeDir(dir, objectName)

	err := m.client.PutObject(dir, objectName, entry.Value)
	if err != nil {
		return err
	}

	return nil
}

// Get is used to fetch an entry
func (m *MantaBackend) Get(key string) (*Entry, error) {
	defer metrics.MeasureSince([]string{"manta", "get"}, time.Now())

	dir, objectName := m.getPathAndObjectName(key)

	resp, err := m.client.GetObject(dir, objectName)
	if err != nil {
		if strings.Contains(err.Error(), "ResourceNotFound") {
			return nil, nil
		}
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("got nil response from manta but no error")
	}

	ent := &Entry{
		Key:   key,
		Value: resp,
	}

	return ent, nil
}

// Delete is used to permanently delete an entry
func (m *MantaBackend) Delete(key string) error {
	defer metrics.MeasureSince([]string{"manta", "delete"}, time.Now())

	dir, objectName := m.getPathAndObjectName(key)

	err := m.client.DeleteObject(dir, objectName)
	if err != nil {
		if strings.Contains(err.Error(), "ResourceNotFound") {
			return nil
		} else if strings.Contains(err.Error(), "DirectoryNotEmpty") {
			m.deleteDirectory(key)
			return nil
		}
		return err
	}

	return nil
}

// List is used to list all the keys under a given
// prefix, up to the next prefix.
func (m *MantaBackend) List(prefix string) ([]string, error) {
	defer metrics.MeasureSince([]string{"manta", "list"}, time.Now())

	entries, err := m.client.ListDirectory(path.Join(m.bucket, prefix), manta.ListDirectoryOpts{})
	if err != nil {
		return nil, nil
	}

	objects := []string{}
	for _, entry := range entries {
		objects = append(objects, entry.Name)
	}

	return objects, nil
}

func (m *MantaBackend) makeDir(dir string, file string) error {
	d, p := path.Split(dir)
	if d == "" {
		m.logger.Debug("root dir : " + p)
		err := m.client.PutDirectory(p)
		if err != nil {
			m.logger.Error(err.Error())
		}
	} else {
		m.logger.Debug(d + p)

		err := m.client.PutDirectory(d + p)
		if err == nil {
			return nil
		}
		if strings.Contains(err.Error(), "DirectoryDoesNotExist") {
			d := path.Clean(d)
			m.makeDir(d, p)
			if err := m.client.PutDirectory(path.Join(d, p)); err != nil {
				m.logger.Error(err.Error())
			}
		}
	}
	return nil
}

func (m *MantaBackend) deleteDirectory(dir string) {
	listResp, _ := m.client.ListDirectory(dir, manta.ListDirectoryOpts{})
	for _, item := range listResp {
		if item.Type == "directory" {
			subDir := path.Join(dir, item.Name)
			m.deleteDirectory(subDir)
			m.client.DeleteDirectory(subDir)
		} else {
			m.client.DeleteObject(dir, item.Name)
		}
	}
}

func (m *MantaBackend) getPathAndObjectName(rawPath string) (string, string) {
	dir, objectName := path.Split(rawPath)
	return path.Join(m.bucket, dir), objectName
}
