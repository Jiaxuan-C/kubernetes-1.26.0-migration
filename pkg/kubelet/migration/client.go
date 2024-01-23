package migration

import (
	"context"
	"encoding/json"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"net/http"
	"os"
	"strings"
	"time"
)

func (m *migrationManager) TriggerPodMigration(pod *v1.Pod) (Result, error) {
	// 初始化HTTP Client
	client, err := getHTTPClient()
	if err != nil {
		panic(err)
	}
	// 这里的clonePod指向的是sourcePod
	sourcePod, err := m.kubeClient.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Labels["CloneSourcePod"], metav1.GetOptions{})
	if err != nil {
		return Result{}, err
	}
	// 获取Pod中所有容器名
	containerList := []string{}
	for _, c := range sourcePod.Spec.Containers {
		containerList = append(containerList, c.Name)
	}

	url := fmt.Sprintf("https://%s:10250/migrate/%s?containers=%s", sourcePod.Status.HostIP, sourcePod.GetUID(), strings.Join(containerList, ","))
	response, err := client.Get(url)
	klog.Infof("===================================================Http GET req was returned: %d===================================================", time.Now().UnixMilli())
	if err != nil {
		return Result{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return Result{}, fmt.Errorf("remote node answered with non-ok status code %v", response.StatusCode)
	}
	res := Result{}
	dec := json.NewDecoder(response.Body)
	if err := dec.Decode(&res); err != nil {
		return Result{}, err
	}

	//time.Sleep(time.Second * 2)

	return res, nil
}

func getHTTPClient() (*http.Client, error) {
	config, err := clientcmd.BuildConfigFromFlags("", "/var/lib/kubelet/kubeconfig")
	if err != nil {
		return nil, err
	}
	tlsConfig, err := rest.TLSConfigFor(config)
	if err != nil {
		return nil, err
	}
	tlsConfig.InsecureSkipVerify = true
	c := &http.Client{}
	c.Transport = &http.Transport{TLSClientConfig: tlsConfig}
	return c, nil
}

func (r *Result) DeleteCheckpoint() {
	if err := os.RemoveAll(r.Path); err != nil {
		klog.Error("failed to delete checkpoint", err)
	}
}
