package migration

import (
	"archive/tar"
	"fmt"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"io"
	"k8s.io/klog/v2"
	"log"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

func downloadDir(sftpClient *sftp.Client, remotePath string, localPath string) {
	// 列出远程目录下的文件和文件夹
	remoteFiles, err := sftpClient.ReadDir(remotePath)
	if err != nil {
		klog.Error("failed to read remote dir", err)
	}

	// 创建本地目录
	if err := os.MkdirAll(localPath, 0755); err != nil {
		klog.Error("failed to mkdir locally", err)
	}

	// 并发下载所有文件和子目录
	var wg sync.WaitGroup
	for _, remoteFile := range remoteFiles {
		remoteFilePath := filepath.Join(remotePath, remoteFile.Name())
		localFilePath := filepath.Join(localPath, remoteFile.Name())

		if remoteFile.IsDir() {
			// 如果是目录，则递归下载其内容
			wg.Add(1)
			go func() {
				defer wg.Done()
				downloadDir(sftpClient, remoteFilePath, localFilePath)
			}()
		} else {
			// 如果是文件，则下载到本地目录
			wg.Add(1)
			go func() {
				defer wg.Done()
				downloadFile(sftpClient, remoteFilePath, localFilePath)
			}()
		}
	}
	wg.Wait()
}

func downloadFile(sftpClient *sftp.Client, remotePath string, localPath string) {
	// 处理软链接
	remoteFileInfo, err := sftpClient.Lstat(remotePath)
	if err != nil {
		klog.Error("failed to list stat of remote dir", err)
	}
	if remoteFileInfo.Mode()&os.ModeSymlink != 0 {
		linkTo, err := sftpClient.ReadLink(remotePath)
		if err != nil {
			klog.Error("failed to read remote symlink", err)
		}
		err = os.Symlink(linkTo, localPath)
		if err != nil {
			log.Panic(err)
		}
	} else {
		// 创建本地文件
		localFile, err := os.Create(localPath)
		if err != nil {
			klog.Error("failed to read create symlink locally", err)
		}
		defer localFile.Close()
		// 打开远程文件
		remoteFile, err := sftpClient.Open(remotePath)
		if err != nil {
			klog.Error("failed to open remote file", err)
		}
		defer remoteFile.Close()

		// 将远程文件内容复制到本地文件
		_, err = io.Copy(localFile, remoteFile)
		if err != nil {
			klog.Error("failed to read copy file", err)
		}
	}
}
func DownloadFullCheckFile(remoteUsrName, remotePassword, remoteNodeIP, remotePath, localPath string) error {
	// SSH连接配置
	sshConfig := &ssh.ClientConfig{
		User: remoteUsrName,
		Auth: []ssh.AuthMethod{
			ssh.Password(remotePassword),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	// 连接node1
	sshClient, err := ssh.Dial("tcp", remoteNodeIP+":22", sshConfig)
	if err != nil {
		return err
	}
	defer sshClient.Close()

	// 创建SFTP客户端
	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		return err
	}
	defer sftpClient.Close()

	// 递归下载/var/lib/kubelet/migration目录下的所有文件
	downloadDir(sftpClient, remotePath, localPath)

	fmt.Println("Download full dump file completed")
	return nil
}

func uploadDir(sftpClient *sftp.Client, localPath string, remotePath string) {
	// 遍历本地目录下的文件和文件夹
	localFiles, err := os.ReadDir(localPath)
	if err != nil {
		klog.Error("failed to read local dir", err)
	}

	// 创建远程目录
	err = sftpClient.MkdirAll(remotePath)
	if err != nil {
		klog.Error("failed to create remote dir", err)
	}

	// 并发上传所有文件和子目录
	var wg sync.WaitGroup
	for _, localFile := range localFiles {
		localFilePath := filepath.Join(localPath, localFile.Name())
		remoteFilePath := filepath.Join(remotePath, localFile.Name())

		if localFile.IsDir() {
			// 如果是目录，则递归上传其内容
			wg.Add(1)
			go func() {
				defer wg.Done()
				uploadDir(sftpClient, localFilePath, remoteFilePath)
			}()
		} else {
			// 如果是文件，则上传到远程目录
			wg.Add(1)
			go func() {
				defer wg.Done()
				uploadFile(sftpClient, localFilePath, remoteFilePath)
			}()
		}
	}
	wg.Wait()
}

func uploadFile(sftpClient *sftp.Client, localPath string, remotePath string) {
	// 处理软链接
	localFileInfo, err := os.Lstat(localPath)
	if err != nil {
		klog.Error("failed to list stat of local file", err)
	}
	if localFileInfo.Mode()&os.ModeSymlink != 0 {
		linkTo, err := os.Readlink(localPath)
		if err != nil {
			klog.Error("failed to read remote symlink", err)
		}
		err = sftpClient.Symlink(linkTo, remotePath)
		if err != nil {
			log.Panic(err)
		}
	} else {
		// 创建远程文件
		remoteFile, err := sftpClient.Create(remotePath)
		if err != nil {
			klog.Error("failed to create remote file", err)
			return
		}
		defer remoteFile.Close()

		// 打开本地文件
		localFile, err := os.Open(localPath)
		if err != nil {
			klog.Error("failed to open local file", err)
			return
		}
		defer localFile.Close()

		// 将本地文件内容复制到远程文件
		_, err = io.Copy(remoteFile, localFile)
		if err != nil {
			klog.Error("failed to copy file content", err)
			return
		}
	}
}

func UploadDumpFile(sftpClient *sftp.Client, localPath, remotePath string) error {
	startTransfer := time.Now().UnixMilli()
	// 递归上传本地目录下的所有文件到远程目录
	fileInfo, err := os.Stat(localPath)
	if err != nil {
		return err
	}
	if fileInfo.IsDir() {
		uploadDir(sftpClient, localPath, remotePath)
	} else {
		uploadFile(sftpClient, localPath, remotePath)
	}

	completeTransfer := time.Now().UnixMilli()
	transferDuration := completeTransfer - startTransfer
	//klog.InfoS("Upload dump file completed: ", strconv.FormatInt(transferDuration, 10), " ms")
	klog.InfoS("===================================================Upload dump file completed: %d", transferDuration, " ms")
	return nil
}
func SftpInit(remoteUsrName, remotePassword, remoteNodeIP string) (*sftp.Client, *ssh.Client, error) {
	// SSH连接配置
	sshConfig := &ssh.ClientConfig{
		User: remoteUsrName,
		Auth: []ssh.AuthMethod{
			ssh.Password(remotePassword),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	// 连接远程服务器
	sshClient, err := ssh.Dial("tcp", remoteNodeIP+":22", sshConfig)
	if err != nil {
		return nil, nil, err
	}
	// 创建SFTP客户端
	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		return nil, nil, err
	}
	return sftpClient, sshClient, nil
}

// RuncPreDump pre-dump the containers.TODO(cjx.bupt)
func RuncPreDump(topPreDumpTimes int, location, containerID string) (int, error) {
	// the first time pre-dump
	IterationTimes := 1
	dirtyPageSizeBuffer := [2]float64{0.0, 0.0}
	runcCmd := exec.Command("runc", "--root", "/run/containerd/runc/k8s.io/", "checkpoint", "--pre-dump", "--image-path", path.Join(location, "preDump1"), containerID)
	if err := runcCmd.Run(); err != nil {
		return 0, fmt.Errorf("runc pre-dump container %q faild: %w, times: %d", containerID, err, 1)
	}
	dirtyPageSizeFirstIter, err := GetDirtyPagesSize(path.Join(location, "preDump1"))
	if err != nil {
		return 0, err
	}
	dirtyPageSizeBuffer[0] = dirtyPageSizeFirstIter
	DPercentage := []float64{}
	for i := 2; i <= topPreDumpTimes; i++ {
		parentPath := "./../preDump" + strconv.Itoa(i-1)
		preDumpPath := path.Join(location, "preDump"+strconv.Itoa(i))
		runcCmd := exec.Command("runc", "--root", "/run/containerd/runc/k8s.io/", "checkpoint",
			"--pre-dump",
			"--image-path", preDumpPath,
			"--parent-path", parentPath,
			containerID)
		if err := runcCmd.Run(); err != nil {
			return 0, fmt.Errorf("runc pre-dump container %q faild: %w, times: %d", containerID, err, i)
		}
		dirtyPageSize, err := GetDirtyPagesSize(preDumpPath)
		if err != nil {
			return 0, err
		}
		IterationTimes = i
		dirtyPageSizeBuffer[1] = dirtyPageSize
		DPercentage := append(DPercentage, math.Abs(100*(dirtyPageSizeBuffer[0]-dirtyPageSizeBuffer[1])/dirtyPageSizeBuffer[0]))
		// TODO(cjx.bupt): weather break or not?
		if BreakOrNot(DPercentage, dirtyPageSize) {
			break
		}
		dirtyPageSizeBuffer[0] = dirtyPageSizeBuffer[1]
	}

	return IterationTimes, nil
}

// BreakOrNot determines whether to continue pre-dump.
func BreakOrNot(DPercentage []float64, dirtyPageSize float64) bool {
	if dirtyPageSize < 2.0 {
		return true
	} else if len(DPercentage) < 4 {
		return false
	} else {
		subDPercentage := DPercentage[len(DPercentage)-5 : len(DPercentage)-1]
		for _, v := range subDPercentage {
			if v > 5.0 {
				break
			}
		}
		return true
	}
}

// GetDirtyPagesSize loads dirtyPages Size.
func GetDirtyPagesSize(preDumpPath string) (float64, error) {
	filePattern := regexp.MustCompile(`^pages-(\d+)\.img$`)
	fileInfos, err := os.ReadDir(preDumpPath)
	dirtyPageSize := 0.0
	if err != nil {
		return dirtyPageSize, fmt.Errorf("get pre-dump path infos failed: %w", err)
	}
	for _, fileInfo := range fileInfos {
		dirtyPageName := fileInfo.Name()
		if dirtyPageName == "parent" {
			continue
		} else if filePattern.MatchString(dirtyPageName) {
			pageStat, err := os.Stat(path.Join(preDumpPath, dirtyPageName))
			if err != nil {
				return 0, fmt.Errorf("get dirty page stat failed: %w", err)
			}
			dirtyPageSize += float64(pageStat.Size() / 1024 / 1024)
		}
	}
	return dirtyPageSize, err
}

// TarContainerRW tar the container rw layer files.
func TarContainerRW(source, target string) error {
	filename := filepath.Base(source)
	target = filepath.Join(target, fmt.Sprintf("%s.tar", filename))

	tarfile, err := os.Create(target)
	if err != nil {
		return err
	}
	defer tarfile.Close()

	tarball := tar.NewWriter(tarfile)
	defer tarball.Close()

	info, err := os.Stat(source)
	if err != nil {
		return nil
	}

	var baseDir string
	if info.IsDir() {
		baseDir = filepath.Base(source)
	}

	return filepath.Walk(source,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			header, err := tar.FileInfoHeader(info, info.Name())
			if err != nil {
				return err
			}

			if baseDir != "" {
				header.Name = filepath.Join(baseDir, strings.TrimPrefix(path, source))
			}

			if err := tarball.WriteHeader(header); err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			_, err = io.Copy(tarball, file)
			return err
		})
}
