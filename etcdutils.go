package etcdutils

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
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
			fmt.Fprintf(os.Stderr, "%v\n", err)
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
		fmt.Printf("etcd client certs already backed up and available %s\n", backupDir)
	}
	if staticDirs, err := filepath.Glob(configFileDir + "/static-pod-resources/kube-apiserver-pod-[0-9]*"); err != nil {
		for _, apiserverPodDir := range staticDirs {
			secretDir := apiserverPodDir + "/secrets/etcd-client"
			configmapDir := apiserverPodDir + "/configmaps/etcd-serving-ca"
			if fileExists(configmapDir+"/ca-bundle.crt") &&
				fileExists(secretDir+"/tls.crt") &&
				fileExists(secretDir+"/tls.key") {
				fmt.Printf("etcd client certs found in %s backing up to %s\n", apiserverPodDir, backupDir)
				copyFile(configmapDir+"/ca-bundle.crt", backupDir+"/etcd-ca-bundle.crt")
				copyFile(secretDir+"/tls.crt", backupDir+"/etcd-client.crt")
				copyFile(secretDir+"/tls.key", backupDir+"/etcd-client.key")
				return nil
			} else {
				fmt.Printf("%s does not contain etcd client certs, trying next ...", apiserverPodDir)
			}
		}
	}
	return errors.New("no etcd client certs found")
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
		log.Fatal(err)
	}
	return err
}

func BackupManifest(manifestDir, assetDir string) error {
	src := manifestDir + "etcd-member.yaml"
	dst := assetDir + "/backup/" + "etcd-member.yaml"
	if fileExists(dst) {
		fmt.Printf("etcd-member.yaml already exists in %s/backup/\n", assetDir)
		return nil
	}
	fmt.Printf("Backing up %s to %s\n", src, dst)
	return copyFile(src, dst)
}

func BackupEtcdConf(assetDir string) error {
	src := "/etc/etcd/etcd.conf"
	dst := assetDir + "/backup/" + "etcd.conf"
	if fileExists(dst) {
		fmt.Printf("etcd.conf backup upready exists in %s/backup/etcd.conf", assetDir)
		return nil
	}
	fmt.Printf("Backing up %s to %s\n", src, dst)
	return copyFile(src, dst)
}

func BackupDataDir(etcdDataDir, assetDir string) error {
	if fileExists(assetDir + "/backup/etcd/member/snap/db") {
		fmt.Fprintf(os.Stderr, "etcd data-dir backup found %s/backup/etcd..\n", etcdDataDir)
		return errors.New("data-dir backup is already found")
	}
	if !fileExists(etcdDataDir + "/member/snap/db") {
		fmt.Fprintf(os.Stderr, "Local etcd snapshot file not found, backup skipped..")
		return errors.New("Local etcd snapshot file not found, backup skipped..")
	}
	return copyDir(etcdDataDir, assetDir+"/backup/etcd")
}

func SnapshotDataDir() {
	// Direct command with etcdctl
}

func BackupCerts(etcdStaticResourceDir, assetDir string) {
	if backupResources, _ := filepath.Glob(assetDir + "/backup/system:etcd-*"); len(backupResources) != 0 {
		fmt.Fprintf(os.Stderr, "etcd TLS certificate backups found in %s/backup..\n", assetDir)
	} else if staticResources, _ := filepath.Glob(etcdStaticResourceDir + "/system:etcd-*"); len(staticResources) != 0 {
		fmt.Println("Backing up etcd certificates..")
		for _, file := range staticResources {
			copyFile(file, assetDir+"/backup/"+filepath.Base(file))
		}
	} else {
		fmt.Fprintf(os.Stderr, "etcd TLS certificates not found, backup skipped..\n")
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

func RestoreSnapshot() {
	// NOT needed, direct etcdctl call
}

func PatchManifest() {
}

func EtcdMemberAdd() {
	// Direct call to etcdctl
}

func StartEtcd(etcdManifest, manifestStoppedDir string) error {
	fmt.Printf("Starting etcd..")
	return os.Rename(manifestStoppedDir+"/etcd-member.yaml", etcdManifest)
}

func EtcdMemberRemove() {
	// Direct call to etcdctl
}

func PopulateTemplate() {
	// Should use goland template format
}

func StartCertRecover(etcdManifest, manifestStoppedDir string) error {
	fmt.Printf("Starting etcd client cert recovery agent..")
	return os.Rename(manifestStoppedDir+"/etcd-generate-certs.yaml", etcdManifest)
}

func VerifyCerts(etcdStaticResourceDir string) {
	staticResources, _ := filepath.Glob(etcdStaticResourceDir + "/system:etcd-*")
	for len(staticResources) < 9 {
		fmt.Printf("Waiting for certs to generate...")
		time.Sleep(10 * time.Second)
		staticResources, _ = filepath.Glob(etcdStaticResourceDir + "/system:etcd-*")
	}
}

func StopCertRecover(etcdManifest, manifestStoppedDir string) error {
	fmt.Printf("Stopping etcd client cert recovery agent..")
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
	fmt.Println("Stopping kubelet..")
	cmd := exec.Command("systemctl", "stop", "kubelet.service")
	return cmd.Run()
}

func StartKubelet() error {
	fmt.Println("Starting kubelet..")
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
