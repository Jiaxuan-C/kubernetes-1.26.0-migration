package migration

import (
	"github.com/emicklei/go-restful/v3"
	"net/http"
	"os"
	"path"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/container"
	kubepod "k8s.io/kubernetes/pkg/kubelet/pod"
)

type Manager interface {
	HandleTargetKubeletRequest(*restful.Request, *restful.Response)
	FindMigrationForPod(*v1.Pod) (Migration, bool)
	TriggerPodMigration(*v1.Pod) (Result, error)
}

type Migration interface {
	Options() *container.MigratePodOptions
	WaitUntilFinished()
}

func NewMigrationManager(kubeClient clientset.Interface, podManager kubepod.Manager, dispatchSyncPodMigrateFn dispatchSyncPodMigrateFunc, rootPath string) Manager {
	return &migrationManager{
		migrationPath:            path.Join(rootPath, "migration"),
		kubeClient:               kubeClient,
		podManager:               podManager,
		dispatchSyncPodMigrateFn: dispatchSyncPodMigrateFn,
		migrations:               make(map[types.UID]*migration),
	}
}

type dispatchSyncPodMigrateFunc func(*v1.Pod)

type migrationManager struct {
	migrationPath            string
	kubeClient               clientset.Interface
	podManager               kubepod.Manager
	dispatchSyncPodMigrateFn dispatchSyncPodMigrateFunc
	migrations               map[types.UID]*migration
}

var _ Manager = &migrationManager{}

type migration struct {
	path       string
	containers []string
	unblock    chan struct{}
	done       chan struct{}
}

type Result struct {
	Path       string
	Containers map[string]CheckpointPath
}

type migrationRequestParams struct {
	podUID         string
	containerNames []string
}

type CheckpointPath = string

var _ Migration = &migration{}

func (m *migrationManager) HandleTargetKubeletRequest(req *restful.Request, res *restful.Response) {
	params := migrationRequestParams{
		podUID:         req.PathParameter("podUID"),
		containerNames: strings.Split(req.QueryParameter("containers"), ","),
	}
	klog.V(2).Infof("POST Migrate - %v %v", params.podUID, params.containerNames)

	var pod *v1.Pod
	var ok bool

	if pod, ok = m.podManager.GetPodByUID(types.UID(params.podUID)); !ok {
		res.WriteHeader(http.StatusNotFound)
		return
	}

	if pod.Status.Phase != v1.PodRunning {
		res.WriteHeader(http.StatusConflict)
		return
	}

	mig := m.newMigration(pod)
	mig.containers = params.containerNames
	mig.IsPathExists()

	klog.V(2).Infof("Starting migration of Pod %v", pod.Name)
	m.dispatchSyncPodMigrateFn(pod)
	// 阻塞
	<-mig.done
	r := Result{
		Path:       mig.path,
		Containers: map[string]CheckpointPath{},
	}
	for _, c := range mig.containers {
		r.Containers[c] = path.Join(mig.path, c)
	}
	if err := res.WriteAsJson(r); err != nil {
		klog.Error("failed to encode migration result.", err)
	}
	// 回复
	res.WriteHeader(http.StatusOK)
	// 写入，此处解锁syncPod中的读锁吗？
	//
	//
	mig.unblock <- struct{}{}
}

func (m *migrationManager) FindMigrationForPod(pod *v1.Pod) (Migration, bool) {
	mig, ok := m.migrations[pod.UID]
	return mig, ok
}

func (m *migrationManager) newMigration(pod *v1.Pod) *migration {
	mig := &migration{
		path:    path.Join(m.migrationPath, string(pod.UID)),
		unblock: make(chan struct{}),
		done:    make(chan struct{}),
	}
	m.migrations[pod.GetUID()] = mig
	return mig
}

func (mg *migration) Options() *container.MigratePodOptions {
	return &container.MigratePodOptions{
		KeepRunning:    false,
		CheckpointsDir: mg.path,
		Unblock:        mg.unblock,
		Done:           mg.done,
		Containers:     mg.containers,
	}
}

func (mg *migration) WaitUntilFinished() {
	<-mg.unblock
}

func (mg *migration) IsPathExists() {
	if err := os.MkdirAll(mg.path, os.FileMode(0755)); err != nil {
		klog.Error("failed to create checkpoint dir", err)
	}
}
