package etcdutils

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
	"time"
)

func Init(assetDir string) error {
	if assetDir == "" {
		assetDir = "."
	}
	dirs := []string{"bin", "tmp", "shared", "backup", "templates", "restore", "manifests"}
	for _, dir := range dirs {
		err := os.Mkdir(assetDir+"/"+dir, os.ModePerm)
		if err != nil && !os.IsExist(err) {
			log.Printf("Eroor creating dir %s: %v\n", dir, err)
			return err
		}
	}
	return nil
}

func BackupEtcdClientCerts(configFileDir, assetDir string) error {
	backupDir := assetDir + "/backup"
	if fileExists(backupDir+"/etcd-ca-bundle.crt") &&
		fileExists(backupDir+"/etcd-client.crt") &&
		fileExists(backupDir+"/etcd-client.key") {
		log.Printf("etcd client certs already backed up and available %s\n", backupDir)
	}
	if staticDirs, err := filepath.Glob(configFileDir + "/static-pod-resources/kube-apiserver-pod-[0-9]*"); err != nil {
		for _, apiserverPodDir := range staticDirs {
			secretDir := apiserverPodDir + "/secrets/etcd-client"
			configmapDir := apiserverPodDir + "/configmaps/etcd-serving-ca"
			if fileExists(configmapDir+"/ca-bundle.crt") &&
				fileExists(secretDir+"/tls.crt") &&
				fileExists(secretDir+"/tls.key") {
				log.Printf("etcd client certs found in %s backing up to %s\n", apiserverPodDir, backupDir)
				copyFile(configmapDir+"/ca-bundle.crt", backupDir+"/etcd-ca-bundle.crt")
				copyFile(secretDir+"/tls.crt", backupDir+"/etcd-client.crt")
				copyFile(secretDir+"/tls.key", backupDir+"/etcd-client.key")
				return nil
			} else {
				log.Printf("%s does not contain etcd client certs, trying next ...\n", apiserverPodDir)
			}
		}
	}
	return fmt.Errorf("no etcd client certs found")
}

func GenConfig(params map[string]string) error {
	var err error
	const configTemplate = `
clusters:
- cluster:
    certificate-authority-data: {{.CA}}
    server: https://{{.RECOVERY_SERVER_IP}}:9943
  name: {{.CLUSTER_NAME}}
contexts:
- context:
    cluster: {{.CLUSTER_NAME}}
    user: kubelet
  name: kubelet
current-context: kubelet
preferences: {}
users:
- name: kubelet
  user:
    client-certificate-data: {{.CERT}}
    client-key-data: {{.KEY}}
`
	t := template.Must(template.New("config").Parse(configTemplate))
	if err = t.Execute(os.Stdout, params); err != nil {
		log.Printf("Error executing template %v\n", err)
	}
	return err
}

func BackupManifest(manifestDir, assetDir string) error {
	src := manifestDir + "etcd-member.yaml"
	dst := assetDir + "/backup/" + "etcd-member.yaml"
	if fileExists(dst) {
		log.Printf("etcd-member.yaml already exists in %s/backup/\n", assetDir)
		return nil
	}
	log.Printf("Backing up %s to %s\n", src, dst)
	return copyFile(src, dst)
}

func BackupEtcdConf(assetDir string) error {
	src := "/etc/etcd/etcd.conf"
	dst := assetDir + "/backup/" + "etcd.conf"
	if fileExists(dst) {
		log.Printf("etcd.conf backup upready exists in %s/backup/etcd.conf\n", assetDir)
		return nil
	}
	log.Printf("Backing up %s to %s\n", src, dst)
	return copyFile(src, dst)
}

func BackupDataDir(etcdDataDir, assetDir string) error {
	if fileExists(assetDir + "/backup/etcd/member/snap/db") {
		log.Printf("etcd data-dir backup found %s/backup/etcd..\n", etcdDataDir)
		return fmt.Errorf("data-dir backup is already found")
	}
	if !fileExists(etcdDataDir + "/member/snap/db") {
		log.Printf("Local etcd snapshot file not found, backup skipped..\n")
		return fmt.Errorf("Local etcd snapshot file not found, backup skipped..")
	}
	return copyDir(etcdDataDir, assetDir+"/backup/etcd")
}

func BackupCerts(etcdStaticResourceDir, assetDir string) {
	if backupResources, _ := filepath.Glob(assetDir + "/backup/system:etcd-*"); len(backupResources) != 0 {
		log.Printf("etcd TLS certificate backups found in %s/backup..\n", assetDir)
	} else if staticResources, _ := filepath.Glob(etcdStaticResourceDir + "/system:etcd-*"); len(staticResources) != 0 {
		log.Println("Backing up etcd certificates..")
		for _, file := range staticResources {
			copyFile(file, assetDir+"/backup/"+filepath.Base(file))
		}
	} else {
		log.Printf("etcd TLS certificates not found, backup skipped..\n")
	}
}

func StopEtcd(etcdManifest, manifestStoppedDir string) error {
	checkAndCreateDir(manifestStoppedDir)
	err := os.Rename(etcdManifest, manifestStoppedDir+"/etcd-member.yaml")
	return err
	// Do we need to wait for stopped etcds?
}

func RemoveDataDir(dataDir string) error {
	return os.RemoveAll(dataDir)
}

func RemoveCerts(etcdStaticResourceDir string) {
	if staticResources, _ := filepath.Glob(etcdStaticResourceDir + "/system:etcd-*"); len(staticResources) != 0 {
		for _, file := range staticResources {
			os.RemoveAll(file)
		}
	}
}

func PatchManifest(manifestFilePath, old, new string) {

	read, err := ioutil.ReadFile(manifestFilePath)
	if err != nil {
		log.Printf("%s:read error %v\n", manifestFilePath, err)
	}
	log.Println(manifestFilePath)

	newContents := strings.Replace(string(read), old, new, -1)

	if err := ioutil.WriteFile(manifestFilePath, []byte(newContents), 0); err != nil {
		log.Printf("%s:write error %s\n", manifestFilePath, err)
	}
}

func StartEtcd(etcdManifest, manifestStoppedDir string) error {
	log.Printf("Starting etcd..\n")
	return os.Rename(manifestStoppedDir+"/etcd-member.yaml", etcdManifest)
}

func PopulateTemplate() {
	// Should use golang template format
}

func StartCertRecover(etcdManifest, manifestStoppedDir string) error {
	log.Printf("Starting etcd client cert recovery agent..\n")
	return os.Rename(manifestStoppedDir+"/etcd-generate-certs.yaml", etcdManifest)
}

func VerifyCerts(etcdStaticResourceDir string) {
	staticResources, _ := filepath.Glob(etcdStaticResourceDir + "/system:etcd-*")
	for len(staticResources) < 9 {
		log.Printf("Waiting for certs to generate...\n")
		time.Sleep(10 * time.Second)
		staticResources, _ = filepath.Glob(etcdStaticResourceDir + "/system:etcd-*")
	}
}

func StopCertRecover(etcdManifest, manifestStoppedDir string) error {
	log.Printf("Stopping etcd client cert recovery agent..\n")
	return os.Rename(etcdManifest, manifestStoppedDir+"/etcd-generate-certs.yaml")
}

func StopStaticPods(etcdManifest, manifestStoppedDir string) error {
	checkAndCreateDir(manifestStoppedDir)
	if fds, err := ioutil.ReadDir(etcdManifest); err == nil {
		for _, fd := range fds {
			if !fd.IsDir() {
				os.Rename(etcdManifest+fd.Name(), manifestStoppedDir+fd.Name())
			}
		}
	} else {
		return err
	}
	return nil
}

func StartStaticPods(etcdManifest, manifestStoppedDir string) error {
	if fds, err := ioutil.ReadDir(manifestStoppedDir); err == nil {
		for _, fd := range fds {
			if !fd.IsDir() {
				os.Rename(manifestStoppedDir+fd.Name(), etcdManifest+fd.Name())
			}
		}
	} else {
		return err
	}
	return nil
}

func StopKubelet() error {
	log.Println("Stopping kubelet..")
	cmd := exec.Command("systemctl", "stop", "kubelet.service")
	return cmd.Run()
}

func StartKubelet() error {
	log.Println("Starting kubelet..")
	cmd := exec.Command("systemctl", "daemon-reload")
	cmd.Run()
	cmd = exec.Command("systemctl", "start", "kubelet.service")
	return cmd.Run()
}

func StopAllContainers() {
}

func ValidateEnvironment() {
}

func ValidateEtcdName() {
}
