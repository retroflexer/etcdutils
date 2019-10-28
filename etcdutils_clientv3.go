package etcdutils

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/clientv3/snapshot"
	"go.uber.org/zap"
)

func SaveSnapshot(ctx context.Context, cfg clientv3.Config, dbPath string) error {
	if len(cfg.Endpoints) != 1 {
		return fmt.Errorf("snapshot must be requested to one selected node, not multiple %#v", cfg.Endpoints)
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return err
	}
	defer cli.Close()

	partpath := dbPath + ".part"
	defer os.RemoveAll(partpath)

	var f *os.File
	f, err = os.OpenFile(partpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileutil.PrivateFileMode)
	if err != nil {
		return fmt.Errorf("could not open %s (%v)", partpath, err)
	}

	now := time.Now()
	var rd io.ReadCloser
	rd, err = cli.Snapshot(ctx)
	if err != nil {
		return err
	}

	if _, err = io.Copy(f, rd); err != nil {
		return err
	}
	if err = fileutil.Fsync(f); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	log.Println(
		"fetched snapshot",
		cfg.Endpoints[0],
		"took", time.Since(now),
	)

	if err = os.Rename(partpath, dbPath); err != nil {
		return fmt.Errorf("could not rename %s to %s (%v)", partpath, dbPath, err)
	}
	log.Println("saved snapshot to path", dbPath)
	return nil
}

func RestoreSnapshot(ctx context.Context, cfg embed.Config, peerURLs []string, dbPath string) error {
	sp := snapshot.NewV3(zap.NewExample())
	return sp.Restore(snapshot.RestoreConfig{
		SnapshotPath:        dbPath,
		Name:                cfg.Name,
		OutputDataDir:       cfg.Dir,
		PeerURLs:            peerURLs,
		InitialCluster:      cfg.InitialCluster,
		InitialClusterToken: cfg.InitialClusterToken,
	})
	return nil
}

func EtcdMemberAdd(ctx context.Context, cfg clientv3.Config, newMemberName string, peerURLs []string) error {
	cli, err := clientv3.New(cfg)
	if err != nil {
		return err
	}
	defer cli.Close()

	mresp, err := cli.MemberAdd(ctx, peerURLs)
	if err != nil {
		return err
	}
	log.Println("added member.PeerURLs:", mresp.Member.PeerURLs)
	return nil
}

func EtcdMemberRemove(ctx context.Context, cfg clientv3.Config, memberName string) error {
	cli, err := clientv3.New(cfg)
	if err != nil {
		return err
	}
	defer cli.Close()

	resp, err := cli.MemberList(ctx)
	if err != nil {
		return err
	}

	var id uint64 = 0
	for _, m := range resp.Members {
		if m.Name == memberName {
			id = m.ID
			break
		}
	}

	if id == 0 {
		return fmt.Errorf("member not found to remove")
	}

	_, err = cli.MemberRemove(ctx, id)
	return err
}
