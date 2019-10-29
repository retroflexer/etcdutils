// etcdutil is a command line application providing the distaster recovery utilities for etcd.
package main

import (
	"fmt"
	"time"
	"strings"
	"context"
	"github.com/retroflexer/etcdutils"
	"github.com/coreos/etcd/clientv3"

	"github.com/spf13/cobra"
)

var (
	memberPeerURLs string
	endPoints string
	dialTimeout time.Duration = 5* time.Second
)

func addMemberCommandFunc(cmd *cobra.Command, args []string) {

	configFileDir := "/etc/kubernetes"
	assetDir := "./assets"
	manifestDir := "/etc/kubernetes/manifests"
	manifestStoppedDir := assetDir + "/manifests-stopped"
	var err error

	// init
	if err = etcdutils.Init(assetDir); err != nil {
		return
	}

	// backup manifest
	if err = etcdutils.BackupManifest(manifestDir, assetDir); err != nil {
		return
	}

	// backup etcdconf
	if err = etcdutils.BackupEtcdConf(assetDir); err != nil {
		return
	}

	// backup client certs 
	if err = etcdutils.BackupEtcdClientCerts(configFileDir, assetDir); err != nil {
		return
	}

	// stop etcd
	if err = etcdutils.StopEtcd(manifestDir, manifestStoppedDir); err != nil {
		return
	}

	cfg:= clientv3.Config{
		Endpoints:   []string{"https://"+args[0]+":2379"},
		DialTimeout: dialTimeout,
	}
	newMemberName := args[1]
	peerURLs := strings.Split(memberPeerURLs, ",")
	etcdutils.EtcdMemberAdd(context.Background(), cfg, newMemberName, peerURLs) 
	return
}

func delMemberCommandFunc(cmd *cobra.Command, args []string) {

	configFileDir := "/etc/kubernetes"
	assetDir := "./assets"
	var err error

	// init
	if err = etcdutils.Init(assetDir); err != nil {
		return
	}

	// backup client certs 
	if err = etcdutils.BackupEtcdClientCerts(configFileDir, assetDir); err != nil {
		return
	}

	// remove the member by name
	cfg:= clientv3.Config{
		Endpoints:   strings.Split(endPoints, ","),
		DialTimeout: dialTimeout,
	}
	memberName := args[0]
	etcdutils.EtcdMemberRemove(context.Background(), cfg, memberName) 
	return
}

func snapshotSaveFunc(cmd *cobra.Command, args []string) {
	cfg:= clientv3.Config{
		Endpoints:   strings.Split(endPoints, ","),
		DialTimeout: dialTimeout,
	}
	dbPath := args[0]

	etcdutils.SaveSnapshot(context.Background(), cfg, dbPath) 
}


func main() {
	var echoTimes int

	var cmdAddMember = &cobra.Command{
		Use:   "addmember <recoveryserverIP> <membername> [options]",
		Short: "Adds a member into the cluster",
		Args: cobra.MinimumNArgs(2),
		Run:   addMemberCommandFunc,
	}

	cmdAddMember.Flags().StringVar(&memberPeerURLs, "peer-urls", "", "comma separated peer URLs for the new member.")

	var cmdDelMember = &cobra.Command{
		Use:   "delmember <membername> [options]",
		Short: "Deletes a member from the cluster",
		Args: cobra.MinimumNArgs(1),
		Run:   delMemberCommandFunc,
	}

	cmdDelMember.Flags().StringVar(&endPoints, "endpoints", "", "comma separated endpoint URLs")

	var cmdSnapshotSave = &cobra.Command{
		Use:   "savesnapshot <filename>",
		Short: "Save snapshot to file specified",
		Args: cobra.MinimumNArgs(1),
		Run: snapshotSaveFunc,
	}
	cmdDelMember.Flags().StringVar(&endPoints, "endpoints", "", "comma separated endpoint URLs")

	var cmdSnapshotRestore = &cobra.Command{
		Use:   "restore <filename>",
		Short: "Restores the database from a file",
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			for i := 0; i < echoTimes; i++ {
				fmt.Println("Echo: " + strings.Join(args, " "))
			}
		},
	}

	var rootCmd = &cobra.Command{Use: "etcdutil"}
	rootCmd.AddCommand(cmdAddMember, cmdDelMember, cmdSnapshotSave, cmdSnapshotRestore)
	rootCmd.Execute()
}


