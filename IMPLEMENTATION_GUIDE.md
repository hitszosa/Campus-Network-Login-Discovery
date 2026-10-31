# CNLD：从 C 基础开始，逐步实现 Go Agent

## 先读这一分钟：为什么这样设计

我们要做的事是：每隔几秒访问认证接口，判断结果，再把最新状态送给 MQTT Broker。

先分成三块：`probecore` 做判断，`main` 发 HTTP 请求并安排时间，`probemqtt` 负责上报。你可以把 core 看成 C 中的 `Result classify(Observation input)`：给同样的输入，就得到同样的输出。它不联网，所以测试很快。

HTTP 和 MQTT 有不同的失败方式。HTTP 超时，只能说“不知道是否认证”；MQTT 断了，只能说“Agent 暂时联系不上”。把这两件事分开，才能知道究竟哪里出了问题。

探测按顺序执行，上报另开一个 goroutine。它们之间放一个只能容纳最新结果的邮箱：Broker 停了，探测照样继续，内存也不会一直涨。重连以后重新探测，让新连接使用新数据。

先做一个进程、一个 Go module。等独立版本跑通，再给相同结果增加 K8s sink。现在学习和排错时，只需要看 HTTP、判断和 MQTT 这条路径。

## 怎么使用这本指导书

每节按“2 分钟读说明、6 分钟写代码、2 分钟验证”安排。共 **35 节，约 5 小时 50 分钟**，可以分几天做；安装工具和首次下载依赖另计。每节写完先看完成标志。

代码块前写“新建”的就是完整文件；写“替换”的就是覆盖该文件。写“追加”时才放在原文件后面。终端命令都从仓库根目录执行。代码中的 module 路径统一为 `github.com/0702hjj/Campus-Network-Login-Discovery`，先照用；若改成自己的地址，所有 import 一起改。

本书提供的是你要亲手实现的参考代码，仓库现在不自动生成 Go 源文件。`cmd/fake-campus`、`cmd/fake-link` 和 `cmd/cnld-watch` 都是实验工具；实际部署的 Agent 不启动它们。

**学习顺序：** 01–05 普通 Go 函数 → 06–10 HTTP 与循环 → 11–12 channel → 13–22 MQTT Agent → 23–29 消费与故障实验 → 30–35 安全部署与验收。

- [01 · 建立 module，运行第一个程序](#01--建立-module运行第一个程序约-10-分钟)
- [02 · 用结构体描述一次观察和一次判断](#02--用结构体描述一次观察和一次判断约-10-分钟)
- [03 · 先检查规则会不会冲突](#03--先检查规则会不会冲突约-10-分钟)
- [04 · 写纯判定函数](#04--写纯判定函数约-10-分钟)
- [05 · 用输入表格测试 core](#05--用输入表格测试-core约-10-分钟)
- [06 · 在本机搭一个假的校园认证接口](#06--在本机搭一个假的校园认证接口约-10-分钟)
- [07 · 让 HTTP 执行器只负责收集事实](#07--让-http-执行器只负责收集事实约-10-分钟)
- [08 · 用 Go 自带的小服务器测试 HTTP](#08--用-go-自带的小服务器测试-http约-10-分钟)
- [09 · 第一次接通 HTTP 和 core](#09--第一次接通-http-和-core约-10-分钟)
- [10 · 改成能取消的周期循环](#10--改成能取消的周期循环约-10-分钟)
- [11 · 用容量为 1 的 channel 保存最新结果](#11--用容量为-1-的-channel-保存最新结果约-10-分钟)
- [12 · 验证循环能记住通知，也能停止](#12--验证循环能记住通知也能停止约-10-分钟)
- [13 · 启动本机 MQTT Broker，认识两个 Topic](#13--启动本机-mqtt-broker认识两个-topic约-10-分钟)
- [14 · 定义 MQTT 报文与配置字段](#14--定义-mqtt-报文与配置字段约-10-分钟)
- [15 · 从 YAML 读取配置](#15--从-yaml-读取配置约-10-分钟)
- [16 · 在开始联网前检查配置](#16--在开始联网前检查配置约-10-分钟)
- [17 · 准备 TLS 证书加载函数](#17--准备-tls-证书加载函数约-10-分钟)
- [18 · 保存底层 socket，等待操作完成](#18--保存底层-socket等待操作完成约-10-分钟)
- [19 · 每次连接创建新 session 和新遗嘱](#19--每次连接创建新-session-和新遗嘱约-10-分钟)
- [20 · 实现一次连接期间的上报循环](#20--实现一次连接期间的上报循环约-10-分钟)
- [21 · 给连接失败加退避，不停止探测](#21--给连接失败加退避不停止探测约-10-分钟)
- [22 · 装配完整 Agent，第一次发布真实报告](#22--装配完整-agent第一次发布真实报告约-10-分钟)
- [23 · 合并报告和 availability，得到当前视图](#23--合并报告和-availability得到当前视图约-10-分钟)
- [24 · 测试重复、乱序、过期和断连](#24--测试重复乱序过期和断连约-10-分钟)
- [25 · 写一个本机订阅工具，定时检查 TTL](#25--写一个本机订阅工具定时检查-ttl约-10-分钟)
- [26 · 验证认证变化与错误分类](#26--验证认证变化与错误分类约-10-分钟)
- [27 · 比较正常退出和崩溃退出](#27--比较正常退出和崩溃退出约-10-分钟)
- [28 · 亲眼看到“报告过期，但连接还没判离线”](#28--亲眼看到报告过期但连接还没判离线约-10-分钟)
- [29 · 用一个丢包代理测试 MQTT 故障隔离](#29--用一个丢包代理测试-mqtt-故障隔离约-10-分钟)
- [30 · 让本机实验先跑通 TLS](#30--让本机实验先跑通-tls约-10-分钟)
- [31 · 添加用户、Topic 权限和 retained 持久化](#31--添加用户topic-权限和-retained-持久化约-10-分钟)
- [32 · 编译二进制，让 systemd 管理它](#32--编译二进制让-systemd-管理它约-10-分钟)
- [33 · 交付 amd64、arm64 二进制与 OCI 镜像](#33--交付-amd64arm64-二进制与-oci-镜像约-10-分钟)
- [34 · 把 MQTT 生命周期实验变成自动测试](#34--把-mqtt-生命周期实验变成自动测试约-10-分钟)
- [35 · 换成真实接口，完成本次实验的验收](#35--换成真实接口完成本次实验的验收约-10-分钟)

读到新语法时，可先用下面的 C 对照：

| Go 写法 | 可以先这样理解 |
| --- | --- |
| `type X struct { ... }` | C 的结构体；字段名以大写开头可跨包访问 |
| `func f(x int) (int, error)` | 返回“结果和错误”两个值，调用后先检查 `err != nil` |
| `x := f()` | 让编译器推断局部变量类型 |
| `[]string` / `map[string]string` | 可变长度字符串序列 / 字符串键值表 |
| `&x` / `*T` / `nil` | 取地址 / 指针类型 / 空值；本书不做指针算术 |
| `defer file.Close()` | 函数返回前关闭资源，类似统一的清理出口 |
| `func (r Result) Labels()` | 第一个参数叫接收者，调用时写 `r.Labels()` |
| `go f()` | 让 Go 运行时并发执行 `f`，需要自己安排何时等待它退出 |
| `ch <- x` / `<-ch` | 向 channel 发送 / 接收；没有准备好时通常会等待 |
| `select` | 选择当前可进行的 channel 操作；`default` 让它不等待 |

需要时再读 [Go 入门教程](https://go.dev/doc/tutorial/getting-started)、[Go module 布局](https://go.dev/doc/modules/layout)。

## 01 · 建立 module，运行第一个程序（约 10 分钟）

先检查 `go version`。本实验使用 Go **1.24 或更新版本**，参考依赖固定为 Paho `v1.5.1` 和 YAML `v3.0.4`，这样不同章节使用同一套 API。

module 是本仓库 Go 代码的统一名字；package 是一个目录里的代码。`package main` 配合 `func main()` 得到可执行程序。

执行：

```sh
go version
go mod init github.com/0702hjj/Campus-Network-Login-Discovery
mkdir -p cmd/cnld-agent cmd/fake-campus cmd/cnld-watch cmd/fake-link
mkdir -p internal/probecore internal/probemqtt configs deploy tests/integration
mkdir -p bin
```

新建 `cmd/cnld-agent/main.go`：

```go
package main

import "fmt"

func main() {
    fmt.Println("CNLD is starting")
}
```

```sh
go run ./cmd/cnld-agent
go test ./...
```

**本节完成标志：** 看到 `CNLD is starting`。空目录此时不需要文件；`go.mod` 需要提交，`bin/` 加到 `.gitignore`。

## 02 · 用结构体描述一次观察和一次判断（约 10 分钟）

`Observation` 是 HTTP 执行器采集的事实，`Result` 是 core 做出的判断。这里先只定义数据。

为什么认证值用字符串？C 的布尔值只有两种，我们需要 `true`、`false`、`unknown` 三种。`type Auth string` 给它起一个专用类型名。

`BodyEquals` 用指针表示“有没有配置”：`nil` 表示不检查 body，指向 `""` 表示必须是空 body。`yaml` 标签是后面配置文件使用的字段名。

**新建 `internal/probecore/types.go`，写入完整内容：**

```go
package probecore

import "time"

type Auth string

const (
	AuthYes     Auth = "true"
	AuthNo      Auth = "false"
	AuthUnknown Auth = "unknown"
)

type Observation struct {
	Code      int
	Body      string
	Location  string
	Truncated bool
	ErrorKind string
	Duration  time.Duration
}

type Rule struct {
	StatusCode int     `yaml:"status_code"`
	BodyEquals *string `yaml:"body_equals"`
}

type Rules struct {
	Authenticated   Rule     `yaml:"authenticated_rule"`
	Unauthenticated Rule     `yaml:"unauthenticated_rule"`
	PortalHosts     []string `yaml:"portal_hosts"`
}

type Result struct {
	Authenticated  Auth
	ExternalAccess string
	Reason         string
	ObservedAt     time.Time
	Duration       time.Duration
}

func (r Result) Labels() map[string]string {
	return map[string]string{
		"network.campus/authenticated":   string(r.Authenticated),
		"network.campus/external_access": r.ExternalAccess,
	}
}
```

```sh
go test ./internal/probecore
```

**本节完成标志：** 包可以编译。现在尝试用一句话解释 `Observation` 和 `Result` 的区别。

## 03 · 先检查规则会不会冲突（约 10 分钟）

例如成功和失败都只检查 `status_code: 200`，程序无法区分。我们把这种问题放到启动时处理。

下面的 `a == nil || *a == x` 利用短路求值：前半部分成立时，不会继续解引用空指针。这和 C 的 `||` 相同。

门户配置只写主机名，例如 `login.example.edu`，不写 `http://`。

**新建 `internal/probecore/rules.go`，写入完整内容：**

```go
package probecore

import (
	"fmt"
	"net/url"
)

func (r Rules) Validate() error {
	a, b := r.Authenticated, r.Unauthenticated
	for _, rule := range []Rule{a, b} {
		if rule.StatusCode < 100 || rule.StatusCode > 599 {
			return fmt.Errorf("status_code must be in 100..599")
		}
	}
	if a.StatusCode == b.StatusCode &&
		(a.BodyEquals == nil || b.BodyEquals == nil || *a.BodyEquals == *b.BodyEquals) {
		return fmt.Errorf("authentication rules overlap")
	}
	if len(r.PortalHosts) > 0 && a.StatusCode >= 300 && a.StatusCode < 400 {
		return fmt.Errorf("success redirect conflicts with portal rules")
	}
	for _, host := range r.PortalHosts {
		u, err := url.Parse("http://" + host)
		if err != nil || u.Hostname() == "" || u.Host != host || u.Path != "" || u.Port() != "" {
			return fmt.Errorf("portal_hosts expects hostnames without scheme or port")
		}
	}
	return nil
}

func matches(rule Rule, o Observation) bool {
	return o.Code == rule.StatusCode && (rule.BodyEquals == nil || *rule.BodyEquals == o.Body)
}
```

```sh
go test ./internal/probecore
```

**本节完成标志：** 能指出哪个 `if` 会拒绝两条相同的规则。规则没有联网，也没有读取文件。

## 04 · 写纯判定函数（约 10 分钟）

现在实现 `Classify`。`switch` 从上往下选一个分支，Go 默认不会像 C 那样继续落入下一个 case。

按这个顺序读：采集错误 → body 超限 → 成功规则 → 明确失败规则 → 已知门户 → 不认识。最后一种保留初始的 `unknown`。当前没有启用独立外网探测，因此 `external_access` 始终为 `unknown`。

时间通过参数 `at` 传进来：真实程序传采样时间，测试传固定时间。

**新建 `internal/probecore/classify.go`，写入完整内容：**

```go
package probecore

import (
	"net/url"
	"strings"
	"time"
)

func Classify(o Observation, rules Rules, at time.Time) Result {
	r := Result{Authenticated: AuthUnknown, ExternalAccess: "unknown",
		Reason: "unrecognized_response", ObservedAt: at, Duration: o.Duration}
	switch {
	case o.ErrorKind != "":
		r.Reason = o.ErrorKind
	case o.Truncated:
		r.Reason = "body_too_large"
	case matches(rules.Authenticated, o):
		r.Authenticated, r.Reason = AuthYes, "login_ok"
	case matches(rules.Unauthenticated, o):
		r.Authenticated, r.Reason = AuthNo, "explicit_unauthenticated"
	default:
		u, err := url.Parse(o.Location)
		if err == nil && (o.Code == 301 || o.Code == 302 || o.Code == 303 || o.Code == 307 || o.Code == 308) {
			for _, host := range rules.PortalHosts {
				if (u.Scheme == "http" || u.Scheme == "https") && strings.EqualFold(u.Hostname(), host) {
					r.Authenticated, r.Reason = AuthNo, "known_portal"
				}
			}
		}
	}
	return r
}
```

```sh
go test ./internal/probecore
```

**本节完成标志：** 给定 302 和一个普通网站地址时，不会得到 `false`。HTTP 200 也必须符合你配置的成功规则。

## 05 · 用输入表格测试 core（约 10 分钟）

文件以 `_test.go` 结尾，函数以 `Test` 开头，`go test` 就会执行它。

每一行包含“场景、输入、预期”。这类似 C 中遍历一组测试结构体。`t.Run` 给失败场景加一个名字。

**新建 `internal/probecore/classify_test.go`，写入完整内容：**

```go
package probecore

import (
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	yes, no := "authenticated", "unauthenticated"
	rules := Rules{Authenticated: Rule{200, &yes}, Unauthenticated: Rule{200, &no},
		PortalHosts: []string{"login.example.edu"}}
	if err := rules.Validate(); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		in   Observation
		want Auth
	}{
		{"yes", Observation{Code: 200, Body: yes}, AuthYes},
		{"no", Observation{Code: 200, Body: no}, AuthNo},
		{"portal", Observation{Code: 302, Location: "https://login.example.edu/"}, AuthNo},
		{"other redirect", Observation{Code: 302, Location: "https://elsewhere.example/"}, AuthUnknown},
		{"timeout", Observation{ErrorKind: "timeout"}, AuthUnknown},
		{"dns", Observation{ErrorKind: "dns_error"}, AuthUnknown},
		{"connection", Observation{ErrorKind: "connection_error"}, AuthUnknown},
		{"unexpected", Observation{Code: 200, Body: "hello"}, AuthUnknown},
		{"truncated", Observation{Code: 200, Body: yes, Truncated: true}, AuthUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Classify(tc.in, rules, at)
			if got.Authenticated != tc.want || !got.ObservedAt.Equal(at) || got.ExternalAccess != "unknown" {
				t.Fatalf("got %+v; want auth=%s", got, tc.want)
			}
		})
	}
	rules.Unauthenticated = rules.Authenticated
	if rules.Validate() == nil {
		t.Fatal("overlapping rules accepted")
	}
}
```

```sh
go test -v ./internal/probecore
```

**本节完成标志：** 所有场景 PASS。把 `AuthUnknown` 改成 `AuthNo` 试一次，看到测试失败后恢复；这样能确认测试确实检查了区别。

## 06 · 在本机搭一个假的校园认证接口（约 10 分钟）

先用能控制的接口学习。这个小服务器读 `/tmp/cnld-mode`，根据文件内容返回不同结果。没有文件时默认已认证。

`http.HandleFunc` 注册路径对应的函数，`ListenAndServe` 开始监听。`w` 写响应，`r` 是请求。所有模拟域名只出现在 Location 中，不需要真的访问它们。

**新建 `cmd/fake-campus/main.go`，写入完整内容：**

```go
package main

import (
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	http.HandleFunc("/check", func(w http.ResponseWriter, r *http.Request) {
		mode, err := os.ReadFile("/tmp/cnld-mode")
		if err != nil {
			mode = []byte("yes")
		}
		switch string(mode) {
		case "yes":
			w.Write([]byte("authenticated"))
		case "no":
			w.Write([]byte("unauthenticated"))
		case "portal":
			http.Redirect(w, r, "http://login.example.edu/", http.StatusFound)
		case "redirect":
			http.Redirect(w, r, "http://other.example.edu/", http.StatusFound)
		case "slow":
			select {
			case <-time.After(10 * time.Second):
			case <-r.Context().Done():
			}
		case "large":
			w.Write(make([]byte, 8192))
		default:
			w.Write([]byte("unexpected"))
		}
	})
	log.Fatal(http.ListenAndServe("127.0.0.1:18080", nil))
}
```

在**终端 A** 运行，后面一直留着：

```sh
go run ./cmd/fake-campus
```

在另一个终端执行；`printf` 不加换行，因为我们会做完整 body 比较：

```sh
printf yes > /tmp/cnld-mode
curl -i http://127.0.0.1:18080/check
printf portal > /tmp/cnld-mode
curl -i http://127.0.0.1:18080/check
printf yes > /tmp/cnld-mode
```

**本节完成标志：** 第一次得到 200 和 `authenticated`；第二次得到 302 和门户 Location。

## 07 · 让 HTTP 执行器只负责收集事实（约 10 分钟）

`http.Client` 可以重复用，底层会管理连接。我们关闭自动跳转，就能看到原本的 302。

`io.LimitReader` 最多给我们 4097 字节：前 4096 是允许内容，多出来的 1 字节用于发现超限。把完整响应一次读进内存会让响应大小决定内存大小。

错误通过 `errors.As` 检查具体类型，通过 `errors.Is` 检查原因；不要靠错误字符串做判断。`context` 在本书里可以先理解成“本次操作的停止通知和截止时间”。参见 [net/http](https://pkg.go.dev/net/http)。

**新建 `cmd/cnld-agent/http.go`，写入完整内容：**

```go
package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/0702hjj/Campus-Network-Login-Discovery/internal/probecore"
)

func newHTTP(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil // 直接测本机出口
	transport.DialContext = (&net.Dialer{Timeout: timeout}).DialContext
	transport.ResponseHeaderTimeout = timeout
	transport.TLSHandshakeTimeout = timeout
	return &http.Client{Transport: transport, Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func observe(ctx context.Context, client *http.Client, endpoint string) probecore.Observation {
	start := time.Now()
	o := probecore.Observation{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		o.ErrorKind = "request_error"
		return o
	}
	resp, err := client.Do(req)
	if err != nil {
		var dns *net.DNSError
		var ne net.Error
		switch {
		case errors.Is(err, context.Canceled):
			o.ErrorKind = "canceled"
		case errors.Is(err, context.DeadlineExceeded):
			o.ErrorKind = "timeout"
		case errors.As(err, &ne) && ne.Timeout():
			o.ErrorKind = "timeout"
		case errors.As(err, &dns):
			o.ErrorKind = "dns_error"
		default:
			o.ErrorKind = "connection_error"
		}
	} else {
		defer resp.Body.Close()
		o.Code = resp.StatusCode
		if location, e := resp.Location(); e == nil {
			o.Location = location.String()
		}
		const limit = 4096
		body, e := io.ReadAll(io.LimitReader(resp.Body, limit+1))
		o.Truncated = len(body) > limit
		if o.Truncated {
			body = body[:limit]
		}
		o.Body = string(body)
		if e != nil {
			o.ErrorKind = "body_read_error"
		}
	}
	o.Duration = time.Since(start)
	return o
}
```

```sh
go test ./cmd/cnld-agent
```

**本节完成标志：** 这个文件没有判断是否认证；它只返回 `Observation`。

## 08 · 用 Go 自带的小服务器测试 HTTP（约 10 分钟）

`httptest.NewServer` 监听临时端口，测试结束就关闭。测试不依赖真实校园网。

`/slow` 等客户端取消请求，帮助我们验证超时真的向下传递了。测试里设置的 100ms 与正式配置互不影响。

**新建 `cmd/cnld-agent/http_test.go`，写入完整内容：**

```go
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestObserve(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, "/target", 302)
		case "/large":
			w.Write(make([]byte, 8192))
		case "/slow":
			<-r.Context().Done()
		default:
			w.Write([]byte("authenticated"))
		}
	}))
	defer server.Close()
	client := newHTTP(100 * time.Millisecond)
	defer client.CloseIdleConnections()
	ctx := context.Background()
	if o := observe(ctx, client, server.URL); o.Body != "authenticated" {
		t.Fatal(o)
	}
	if o := observe(ctx, client, server.URL+"/redirect"); o.Code != 302 || o.Location != server.URL+"/target" {
		t.Fatal(o)
	}
	if o := observe(ctx, client, server.URL+"/large"); !o.Truncated || len(o.Body) != 4096 {
		t.Fatal("body limit")
	}
	if o := observe(ctx, client, server.URL+"/slow"); o.ErrorKind != "timeout" {
		t.Fatal(o)
	}
}
```

```sh
go test -v ./cmd/cnld-agent
```

**本节完成标志：** 普通 body、重定向、超限、超时四种行为都通过。

## 09 · 第一次接通 HTTP 和 core（约 10 分钟）

现在替换 Hello World，做一次真实的本机探测。`at` 在 HTTP 开始前记录，作为本次观察时间。标签的 JSON 不包含原始 body。

**替换 `cmd/cnld-agent/main.go`：**

```go
package main

import (
 "context"
 "encoding/json"
 "fmt"
 "time"

 "github.com/0702hjj/Campus-Network-Login-Discovery/internal/probecore"
)

func main() {
 yes,no:="authenticated","unauthenticated"
 rules:=probecore.Rules{Authenticated:probecore.Rule{StatusCode:200,BodyEquals:&yes},
  Unauthenticated:probecore.Rule{StatusCode:200,BodyEquals:&no},PortalHosts:[]string{"login.example.edu"}}
 client:=newHTTP(time.Second)
 defer client.CloseIdleConnections()
 at:=time.Now()
 o:=observe(context.Background(),client,"http://127.0.0.1:18080/check")
 r:=probecore.Classify(o,rules,at)
 data,err:=json.Marshal(r.Labels())
 if err!=nil { panic(err) }
 fmt.Println(string(data),r.Reason)
}
```

```sh
printf yes > /tmp/cnld-mode
go run ./cmd/cnld-agent
printf no > /tmp/cnld-mode
go run ./cmd/cnld-agent
printf slow > /tmp/cnld-mode
go run ./cmd/cnld-agent
printf yes > /tmp/cnld-mode
```

**本节完成标志：** 分别得到 `true / false / unknown`。每次执行后程序自行结束；这里只做单次探测。

## 10 · 改成能取消的周期循环（约 10 分钟）

先不用 goroutine。主函数调用循环，每轮 HTTP 结束后，再等下一次定时事件。

`defer ticker.Stop()` 回收定时器。`select` 等待三个来源：退出、周期到达、稍后 MQTT 发来的重新采样通知。`nil` channel 永远不会就绪，现在可以先不启用第三种来源。

`sink` 表示“只要有 Submit 方法，就能收结果”。Go 不需要写 `implements`。今天先只打印，后面把 MQTT 接进同一个函数。

**新建 `cmd/cnld-agent/run.go`，写入完整内容：**

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/0702hjj/Campus-Network-Login-Discovery/internal/probecore"
)

type sink interface{ Submit(probecore.Result) }

func runProbe(ctx context.Context, interval time.Duration, trigger <-chan struct{},
	sample func(context.Context) probecore.Result, outputs ...sink) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	previous := probecore.Auth("")
	for {
		if ctx.Err() != nil {
			return
		}
		result := sample(ctx)
		if ctx.Err() != nil {
			return
		}
		log.Printf("probe auth=%s reason=%s changed=%t", result.Authenticated, result.Reason, result.Authenticated != previous)
		previous = result.Authenticated
		for _, output := range outputs {
			output.Submit(result)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-trigger:
		}
	}
}
```

**替换 `cmd/cnld-agent/main.go`：**

```go
package main

import (
 "context"
 "os"
 "os/signal"
 "syscall"
 "time"

 "github.com/0702hjj/Campus-Network-Login-Discovery/internal/probecore"
)

func main() {
 yes,no:="authenticated","unauthenticated"
 rules:=probecore.Rules{Authenticated:probecore.Rule{StatusCode:200,BodyEquals:&yes},
  Unauthenticated:probecore.Rule{StatusCode:200,BodyEquals:&no},PortalHosts:[]string{"login.example.edu"}}
 client:=newHTTP(time.Second)
 defer client.CloseIdleConnections()
 ctx,stop:=signal.NotifyContext(context.Background(),os.Interrupt,syscall.SIGTERM)
 defer stop()
 sample:=func(ctx context.Context) probecore.Result {
  at:=time.Now()
  probeCtx,cancel:=context.WithTimeout(ctx,time.Second)
  defer cancel()
  return probecore.Classify(observe(probeCtx,client,"http://127.0.0.1:18080/check"),rules,at)
 }
 runProbe(ctx,3*time.Second,nil,sample)
}
```

```sh
go run ./cmd/cnld-agent
```

**本节完成标志：** 启动马上打印一次，然后约每 3 秒打印；修改 `/tmp/cnld-mode` 后下一轮变化。Ctrl+C 能结束正在等待的循环。

## 11 · 用容量为 1 的 channel 保存最新结果（约 10 分钟）

goroutine 之间需要传递结果。现在准备一个邮箱，先单独测试它。

普通 `ch <- r` 在 channel 满时会等待。`select` 加 `default` 可以立即发现满了；取走旧结果，再放入新结果，就始终只留一个。

这段代码的使用方式很具体：**只有串行探测循环发送，只有上报 worker 接收**。不要从 HTTP 回调或多个 goroutine 同时调用 `Submit`。下一节会验证这个约定下结果不会积压。

**新建 `internal/probemqtt/mailbox.go`，写入完整内容：**

```go
package probemqtt

import "github.com/0702hjj/Campus-Network-Login-Discovery/internal/probecore"

// 只有串行探测循环调用 Submit；worker 只接收。
type Mailbox struct{ C chan probecore.Result }

func NewMailbox() *Mailbox { return &Mailbox{C: make(chan probecore.Result, 1)} }

func (m *Mailbox) Submit(r probecore.Result) {
	select {
	case m.C <- r:
		return
	default:
	}
	select {
	case <-m.C:
	default:
	} // 有旧结果就取走
	m.C <- r // 单一发送者；现在必有空位
}
```

**新建 `internal/probemqtt/mailbox_test.go`，写入完整内容：**

```go
package probemqtt

import (
	"github.com/0702hjj/Campus-Network-Login-Discovery/internal/probecore"
	"testing"
)

func TestLatest(t *testing.T) {
	m := NewMailbox()
	for i := 0; i < 10000; i++ {
		m.Submit(probecore.Result{Reason: "old"})
	}
	m.Submit(probecore.Result{Reason: "latest"})
	if got := <-m.C; got.Reason != "latest" {
		t.Fatal(got)
	}
	if len(m.C) != 0 {
		t.Fatal("mailbox should be empty")
	}
}
```

```sh
go test -v ./internal/probemqtt
```

**本节完成标志：** 发送 10001 次也能很快完成，接收时只有 `latest`。此时 Agent 仍只打印日志。

## 12 · 验证循环能记住通知，也能停止（约 10 分钟）

本测试用两个 channel 控制采样开始和结束。它不靠 `Sleep(3s)` 猜时间，因此运行很快。

`go func(){...}()` 在测试中启动循环。`done` 用来告诉主测试“循环已经结束”。`close(done)` 后，`<-done` 可以继续，和 C 中等待线程完成的用途相似。

先记住一条写法：**启动后台工作时，同时准备等待它退出的办法。**

**新建 `cmd/cnld-agent/run_test.go`，写入完整内容：**

```go
package main

import (
	"context"
	"testing"
	"time"

	"github.com/0702hjj/Campus-Network-Login-Discovery/internal/probecore"
)

type recorder struct{ C chan probecore.Result }

func (r recorder) Submit(v probecore.Result) { r.C <- v }

func TestRunProbe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trigger := make(chan struct{}, 1)
	entered, release := make(chan struct{}, 2), make(chan struct{})
	output := recorder{make(chan probecore.Result, 2)}
	done := make(chan struct{})
	sample := func(ctx context.Context) probecore.Result {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return probecore.Result{Authenticated: probecore.AuthYes}
	}
	go func() { defer close(done); runProbe(ctx, time.Hour, trigger, sample, output) }()
	<-entered             // 启动即探测；第一次仍在执行
	trigger <- struct{}{} // 重连通知留在 channel，等第一次结束再处理
	release <- struct{}{}
	<-output.C
	<-entered // 不用等一小时，通知立即触发第二次
	cancel()  // 第二次采样可被取消，且不会提交第二份报告
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probe did not stop")
	}
	if len(output.C) != 0 {
		t.Fatal("published after cancel")
	}
}
```

```sh
go test -race ./cmd/cnld-agent ./internal/probemqtt
```

**本节完成标志：** 第一次采样没结束时发出的通知，会在之后触发第二次；取消第二次采样后不再提交结果。`-race` 检查运行时出现的数据竞争。

## 13 · 启动本机 MQTT Broker，认识两个 Topic（约 10 分钟）

本机实验使用 Mosquitto。Ubuntu/Debian 可执行下列安装命令；Arch 使用 `sudo pacman -S mosquitto`；已有工具可跳过安装。Broker、发布和订阅是三个角色：Agent 发消息，Broker 转发，消费者接收。

实验 Broker 只绑定回环地址。创建 `deploy/mosquitto-lab.conf`：

```text
listener 1883 127.0.0.1
allow_anonymous true
persistence false
```

```sh
sudo apt-get install mosquitto mosquitto-clients
sudo systemctl stop mosquitto
```

在**终端 B** 启动（如果系统服务已占用 1883，先停止该实验机上的服务，再启动此配置）：

```sh
mosquitto -c deploy/mosquitto-lab.conf -v
```

在**终端 C** 订阅：

```sh
mosquitto_sub -h 127.0.0.1 -q 1 -t 'cnld/nodes/lab-01/#' -v
```

在另一个终端手动发送一条实验消息，再重启订阅者：

```sh
mosquitto_pub -h 127.0.0.1 -t cnld/lab/demo -m hello -q 1 -r
mosquitto_sub -h 127.0.0.1 -t cnld/lab/demo -C 1
```

**本节完成标志：** 新订阅者立即读到 `hello`。`-r` 保存最后一条消息，`-q 1` 允许至少一次投递；因此消费者后面要处理重复。协议参考：[MQTT 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)。

## 14 · 定义 MQTT 报文与配置字段（约 10 分钟）

认证结果和连接状态分两个 Topic。报告里的 `connection_id` 说明属于哪次连接，`sequence` 说明这次连接中的第几份报告，`observed_at + valid_for_seconds` 说明什么时候过期。

结构体后面的 `json` 标签控制 JSON 字段名。这里还定义 MQTT 配置，下一节直接从 YAML 加载。

**新建 `internal/probemqtt/messages.go`，写入完整内容：**

```go
package probemqtt

import "time"

type Availability struct {
	SchemaVersion int    `json:"schema_version"`
	NodeID        string `json:"node_id"`
	ConnectionID  string `json:"connection_id"`
	Status        string `json:"status"`
	Reason        string `json:"reason"`
}

type Report struct {
	SchemaVersion   int               `json:"schema_version"`
	NodeID          string            `json:"node_id"`
	ConnectionID    string            `json:"connection_id"`
	Sequence        uint64            `json:"sequence"`
	ObservedAt      time.Time         `json:"observed_at"`
	ValidForSeconds int64             `json:"valid_for_seconds"`
	Labels          map[string]string `json:"labels"`
	Reason          string            `json:"reason"`
	ProbeDurationMS int64             `json:"probe_duration_ms"`
}

type Config struct {
	Broker         string        `yaml:"broker"`
	TopicPrefix    string        `yaml:"topic_prefix"`
	ClientID       string        `yaml:"client_id"`
	KeepAlive      time.Duration `yaml:"keep_alive"`
	PublishTimeout time.Duration `yaml:"publish_timeout"`
	MaxBackoff     time.Duration `yaml:"max_backoff"`
	UsernameEnv    string        `yaml:"username_env"`
	PasswordEnv    string        `yaml:"password_env"`
	CAFile         string        `yaml:"ca_file"`
	CertFile       string        `yaml:"cert_file"`
	KeyFile        string        `yaml:"key_file"`
}
```

```sh
go test ./internal/probemqtt
```

**本节完成标志：** 能在代码里找到“观察时间”和“连接标识”，并解释为什么重连不能只改时间后重发旧报告。

## 15 · 从 YAML 读取配置（约 10 分钟）

先安装 YAML 解析库。`KnownFields(true)` 会指出拼错的字段，例如 `intervel`。时间直接写 `3s`，库会转换到 `time.Duration`。

先创建加载器和实验配置；本节用一个小的 `validate` 占位函数，下一节替换为实际校验。

```sh
go get go.yaml.in/yaml/v3@v3.0.4
```

**新建 `cmd/cnld-agent/config.go`，写入完整内容：**

```go
package main

import (
	"os"
	"time"

	"github.com/0702hjj/Campus-Network-Login-Discovery/internal/probecore"
	"github.com/0702hjj/Campus-Network-Login-Discovery/internal/probemqtt"
	"go.yaml.in/yaml/v3"
)

type ProbeConfig struct {
	Endpoint string          `yaml:"endpoint"`
	Interval time.Duration   `yaml:"interval"`
	Timeout  time.Duration   `yaml:"timeout"`
	ValidFor time.Duration   `yaml:"valid_for"`
	Rules    probecore.Rules `yaml:",inline"`
}

type Config struct {
	NodeID string           `yaml:"node_id"`
	Sinks  []string         `yaml:"sinks"`
	Probe  ProbeConfig      `yaml:"probe"`
	MQTT   probemqtt.Config `yaml:"mqtt"`
}

func loadConfig(path string) (Config, error) {
	var c Config
	file, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer file.Close()
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(&c); err != nil {
		return c, err
	}
	if c.MQTT.TopicPrefix == "" {
		c.MQTT.TopicPrefix = "cnld/nodes"
	}
	if c.MQTT.ClientID == "" {
		c.MQTT.ClientID = "cnld-" + c.NodeID
	}
	if c.MQTT.KeepAlive == 0 {
		c.MQTT.KeepAlive = 30 * time.Second
	}
	if c.MQTT.PublishTimeout == 0 {
		c.MQTT.PublishTimeout = 5 * time.Second
	}
	if c.MQTT.MaxBackoff == 0 {
		c.MQTT.MaxBackoff = 30 * time.Second
	}
	return c, c.validate()
}
```

新建 `cmd/cnld-agent/validate.go`，本节暂写：

```go
package main

func (c Config) validate() error {
    return nil
}
```

**新建 `configs/lab.yaml`，写入完整内容：**

```yaml
node_id: lab-01
sinks: [mqtt]
probe:
  endpoint: http://127.0.0.1:18080/check
  interval: 3s
  timeout: 1s
  valid_for: 10s
  authenticated_rule:
    status_code: 200
    body_equals: authenticated
  unauthenticated_rule:
    status_code: 200
    body_equals: unauthenticated
  portal_hosts: [login.example.edu]
mqtt:
  broker: tcp://127.0.0.1:1883
  keep_alive: 4s
  publish_timeout: 2s
  max_backoff: 8s
```

```sh
go test ./...
```

**本节完成标志：** 配置文件准备好了；Agent 主函数尚未切换到配置，所以运行结果暂时不变。

## 16 · 在开始联网前检查配置（约 10 分钟）

现在替换占位函数。用两个例子理解校验：

- 探测超时 1 秒、周期 3 秒、有效期 10 秒：正常探测和传输留有时间。
- 超时 10 秒、周期 3 秒：请求还没结束，下个周期就到了。本实验直接指出错误，让配置更容易观察。

`os.LookupEnv` 检查指定的凭证变量是否存在，只输出变量名，不输出值。本期只支持 `[mqtt]`；写 `[k8s]` 会清楚报错。

**替换 `cmd/cnld-agent/validate.go` 的完整内容：**

```go
package main

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

func (c Config) validate() error {
	if !regexp.MustCompile(`^[A-Za-z0-9._-]+$`).MatchString(c.NodeID) {
		return fmt.Errorf("invalid node_id")
	}
	if len(c.Sinks) != 1 || c.Sinks[0] != "mqtt" {
		return fmt.Errorf("this build supports sinks: [mqtt] only")
	}
	u, err := url.Parse(c.Probe.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return fmt.Errorf("probe.endpoint must be an http(s) URL without credentials")
	}
	p := c.Probe
	if p.Timeout <= 0 || p.Interval <= p.Timeout || p.ValidFor <= p.Interval+p.Timeout || p.ValidFor%time.Second != 0 {
		return fmt.Errorf("need 0 < timeout < interval; valid_for > interval + timeout, in whole seconds")
	}
	if err := p.Rules.Validate(); err != nil {
		return err
	}
	b, err := url.Parse(c.MQTT.Broker)
	if err != nil || b.Hostname() == "" || b.Port() == "" || b.User != nil || b.Path != "" || b.RawQuery != "" || b.Fragment != "" ||
		(b.Scheme != "tcp" && b.Scheme != "tls" && b.Scheme != "ssl") {
		return fmt.Errorf("mqtt.broker must be tcp/tls/ssl://host:port")
	}
	m := c.MQTT
	if m.TopicPrefix == "" || strings.ContainsAny(m.TopicPrefix, "+#") || strings.HasSuffix(m.TopicPrefix, "/") {
		return fmt.Errorf("invalid topic_prefix")
	}
	if m.KeepAlive < time.Second || m.KeepAlive > 65535*time.Second || m.KeepAlive%time.Second != 0 ||
		m.PublishTimeout <= 0 || m.MaxBackoff < time.Second {
		return fmt.Errorf("invalid MQTT timing")
	}
	for _, name := range []string{m.UsernameEnv, m.PasswordEnv} {
		if name != "" {
			if _, ok := os.LookupEnv(name); !ok {
				return fmt.Errorf("missing environment variable %s", name)
			}
		}
	}
	return nil
}
```

新建 `cmd/cnld-agent/config_test.go`：

```go
package main

import (
    "testing"
    "time"
)

func TestConfig(t *testing.T) {
    c, err := loadConfig("../../configs/lab.yaml")
    if err != nil { t.Fatal(err) }
    c.Probe.Timeout = 5 * time.Second
    if c.validate() == nil { t.Fatal("timeout >= interval accepted") }
}
```

```sh
go test ./cmd/cnld-agent
```

**本节完成标志：** 实验配置能加载；超时改成 5 秒会被拒绝。先不修改 `lab.yaml`，用测试验证即可。

## 17 · 准备 TLS 证书加载函数（约 10 分钟）

本机 Broker 暂用 TCP，部署到远程时切换 TLS。先写好配置加载函数，后面连接层就不需要关心证书文件。

`CAFile` 增加受信任的签发者；`CertFile` 和 `KeyFile` 是可选客户端身份。没有自定义 CA 时使用系统信任库。TLS 会检查服务端身份，不设置跳过验证的选项。

**新建 `internal/probemqtt/tls.go`，写入完整内容：**

```go
package probemqtt

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func TLSConfig(c Config) (*tls.Config, error) {
	t := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		roots, err := x509.SystemCertPool()
		if err != nil {
			return nil, err
		}
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, err
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("CA file has no certificates")
		}
		t.RootCAs = roots
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return nil, fmt.Errorf("cert_file and key_file must be paired")
	}
	if c.CertFile != "" {
		pair, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, err
		}
		t.Certificates = []tls.Certificate{pair}
	}
	return t, nil
}
```

```sh
go test ./internal/probemqtt
```

**本节完成标志：** 配置代码和联网代码仍分开。证书内容没有进入 MQTT 报告。

## 18 · 保存底层 socket，等待操作完成（约 10 分钟）

安装固定版本的 Go MQTT 客户端：

```sh
go get github.com/eclipse/paho.mqtt.golang@v1.5.1
```

正常 MQTT DISCONNECT 会取消遗嘱。如果退出时 offline 没发成功，我们需要直接关闭 TCP 连接，让 Broker 仍有机会发布遗嘱。因此保留一份底层 `net.Conn`。

`sync.Mutex` 的用法很像 C 的互斥锁：锁保护 `conn` 和 `closed`。锁住的范围只做内存更新和关闭，不把整个 Dial 放在锁内。

Paho 的 `Token` 表示一次异步操作。`wait` 等完成、取消或超时三者之一。它只停止等待；关闭 socket 才会让底层网络操作结束。API 参考：[Paho v1.5.1](https://pkg.go.dev/github.com/eclipse/paho.mqtt.golang@v1.5.1#ClientOptions.SetCustomOpenConnectionFn)。

**新建 `internal/probemqtt/wire.go`，写入完整内容：**

```go
package probemqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// 保存底层 socket；Close 不发送 MQTT DISCONNECT。
type wire struct {
	mu     sync.Mutex
	conn   net.Conn
	closed bool
}

func (w *wire) open(uri *url.URL, options mqtt.ClientOptions) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), options.ConnectTimeout)
	defer cancel()
	d := &net.Dialer{}
	var conn net.Conn
	var err error
	switch uri.Scheme {
	case "tcp":
		conn, err = d.DialContext(ctx, "tcp", uri.Host)
	case "tls", "ssl":
		td := &tls.Dialer{NetDialer: d, Config: options.TLSConfig}
		conn, err = td.DialContext(ctx, "tcp", uri.Host)
	default:
		return nil, fmt.Errorf("unsupported broker scheme")
	}
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		conn.Close()
		return nil, fmt.Errorf("connection attempt canceled")
	}
	w.conn = conn
	return conn, nil
}

func (w *wire) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	if w.conn != nil {
		w.conn.Close()
	}
}

func wait(ctx context.Context, token mqtt.Token, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("mqtt operation timed out")
	case <-token.Done():
		return token.Error()
	}
}
```

```sh
go test ./internal/probemqtt
```

**本节完成标志：** `wire.close()` 只关闭 socket，不发送 MQTT 报文；已经取消的 Dial 即使晚完成，也会立即关闭新连接。

## 19 · 每次连接创建新 session 和新遗嘱（约 10 分钟）

一次连接对应一个 `session`。随机连接 ID 在 CONNECT 之前生成，写进遗嘱；正常 online、report、offline 都用它。

Paho 的自动重连在这里关闭。由下一节 worker 重新创建 session，就能给每次连接换一份遗嘱。连接断开回调只向 channel 发通知，不直接开始探测或发布消息。

这节的 `session` 是包内类型，首字母小写，外部包不能直接操作它。

**新建 `internal/probemqtt/session.go`，写入完整内容：**

```go
package probemqtt

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type session struct {
	client   mqtt.Client
	wire     *wire
	lost     chan struct{}
	id       string
	readyAt  time.Time
	sequence uint64
}

func newSession(c Config, node string, tlsConfig *tls.Config) (*session, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	s := &session{wire: &wire{}, lost: make(chan struct{}, 1), id: hex.EncodeToString(id[:])}
	will, err := json.Marshal(Availability{1, node, s.id, "offline", "connection_lost"})
	if err != nil {
		return nil, err
	}
	options := mqtt.NewClientOptions().AddBroker(c.Broker).SetClientID(c.ClientID).
		SetProtocolVersion(4).SetCleanSession(true).SetAutoReconnect(false).SetConnectRetry(false).
		SetKeepAlive(c.KeepAlive).SetPingTimeout(c.PublishTimeout).
		SetConnectTimeout(c.PublishTimeout).SetWriteTimeout(c.PublishTimeout).
		SetTLSConfig(tlsConfig).SetCustomOpenConnectionFn(s.wire.open).
		SetUsername(os.Getenv(c.UsernameEnv)).SetPassword(os.Getenv(c.PasswordEnv)).
		SetBinaryWill(c.TopicPrefix+"/"+node+"/availability", will, 1, true)
	options.SetConnectionLostHandler(func(mqtt.Client, error) {
		select {
		case s.lost <- struct{}{}:
		default:
		}
	})
	s.client = mqtt.NewClient(options)
	return s, nil
}

func (s *session) publish(ctx context.Context, c Config, topic string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return wait(ctx, s.client.Publish(topic, 1, true, body), c.PublishTimeout)
}

func (s *session) availability(ctx context.Context, c Config, node, status, reason string) error {
	return s.publish(ctx, c, c.TopicPrefix+"/"+node+"/availability",
		Availability{1, node, s.id, status, reason})
}

func (s *session) abort() {
	s.wire.close()         // 顺序关键：先关闭 socket，Broker 才有机会发布遗嘱
	s.client.Disconnect(0) // 只清理库的资源；socket 已关，不能再发送 DISCONNECT
}
```

```sh
go test ./internal/probemqtt
```

**本节完成标志：** 找到 `SetProtocolVersion(4)`：Paho 的 4 代表 MQTT 3.1.1。找到 Will 的 QoS=1、retain=true。

## 20 · 实现一次连接期间的上报循环（约 10 分钟）

上报 worker 的主要顺序是：CONNECT → online 被确认 → 请求新采样 → 收报告并发送。

注意 `readyAt` 的位置。邮箱可能还留着旧结果，也可能有一轮旧采样尚未完成。我们比较“采样开始时间”与 `readyAt`，这样两种旧结果都不会被换上新连接 ID。

同一进程中的 `time.Time` 保留单调时钟部分，可用于这个先后比较；编码到 JSON 时才转换 UTC。见 [time 文档](https://pkg.go.dev/time#hdr-Monotonic_Clocks)。

`shutdown` 用新的清理 context：因为运行 context 已取消，继续用它发 offline 会立即失败。

**新建 `internal/probemqtt/worker.go`，写入完整内容：**

```go
package probemqtt

import (
	"context"
	"crypto/tls"
	"log"
	"time"
)

type Worker struct {
	*Mailbox
	Config   Config
	Node     string
	ValidFor time.Duration
	TLS      *tls.Config
	Trigger  chan<- struct{}
}

func (w *Worker) once(ctx context.Context) (bool, error) {
	c := w.Config
	s, err := newSession(c, w.Node, w.TLS)
	if err != nil {
		return false, err
	}
	defer s.abort()
	if err := wait(ctx, s.client.Connect(), c.PublishTimeout); err != nil {
		return false, err
	}
	if err := s.availability(ctx, c, w.Node, "online", "connected"); err != nil {
		return false, err
	}
	s.readyAt = time.Now() // 保留单调时钟部分，只接收此后开始采样的结果
	log.Printf("node=%s mqtt online connection_id=%s", w.Node, s.id)
	select {
	case w.Trigger <- struct{}{}:
	default:
	}
	for {
		if ctx.Err() != nil {
			w.shutdown(s)
			return true, nil
		}
		select {
		case <-ctx.Done():
			w.shutdown(s)
			return true, nil
		case <-s.lost:
			return true, context.Canceled
		case r := <-w.C:
			if r.ObservedAt.Before(s.readyAt) || time.Now().After(r.ObservedAt.Add(w.ValidFor)) {
				continue
			}
			s.sequence++
			report := Report{1, w.Node, s.id, s.sequence, r.ObservedAt.UTC(), int64(w.ValidFor / time.Second),
				r.Labels(), r.Reason, r.Duration.Milliseconds()}
			if err := s.publish(ctx, c, c.TopicPrefix+"/"+w.Node+"/report", report); err != nil {
				if ctx.Err() != nil {
					w.shutdown(s)
				}
				return true, err
			}
		}
	}
}

func (w *Worker) shutdown(s *session) {
	// runProbe 与 worker 收到同一取消信号；本 worker 从此不再消费报告。
	ctx, cancel := context.WithTimeout(context.Background(), w.Config.PublishTimeout)
	defer cancel()
	if err := s.availability(ctx, w.Config, w.Node, "offline", "graceful_shutdown"); err != nil {
		log.Printf("node=%s offline not confirmed; closing transport", w.Node)
		return // defer s.abort() 直接关闭 socket，留给 Broker 发 Will
	}
	s.client.Disconnect(100) // 只在 offline 已确认后正常断开
}
```

```sh
go test ./internal/probemqtt
```

**本节完成标志：** MQTT worker 只读邮箱；它没有调用 HTTP。所有 publish 都等待 ACK 后才继续，最多只有一份报告在途。

## 21 · 给连接失败加退避，不停止探测（约 10 分钟）

如果 Broker 停了，每次立刻重连会产生很多连接请求。这里让连续失败后的等待时间增加，但最多等配置里的 `max_backoff`。

“抖动”就是把实际等待随机放在 `[delay/2, delay]` 内，多台机器就不容易同时重连。定时等待也监听 `ctx.Done()`，退出时无需等完整退避结束。

**新建 `internal/probemqtt/retry.go`，写入完整内容：**

```go
package probemqtt

import (
	"context"
	"log"
	"math/rand/v2"
	"time"
)

func (w *Worker) Run(ctx context.Context) {
	delay := time.Second
	for ctx.Err() == nil {
		connected, err := w.once(ctx)
		if ctx.Err() != nil {
			return
		}
		if connected {
			delay = time.Second
		}
		log.Printf("node=%s mqtt retry_in<=%s error=%v", w.Node, delay, err)
		// 等待时间在 [delay/2, delay] 内；连续失败时翻倍，到 MaxBackoff 为止。
		pause := delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(delay*2, w.Config.MaxBackoff)
	}
}
```

```sh
go test ./internal/probemqtt
```

**本节完成标志：** 连接失败后只影响 `Worker.Run`，不影响主 goroutine 的探测循环。

## 22 · 装配完整 Agent，第一次发布真实报告（约 10 分钟）

现在用配置替换硬编码，并真正启动一个上报 goroutine。

主 goroutine 执行 `runProbe`；后台 goroutine 执行 `worker.Run`。它们收到同一个取消信号后各自停止，主函数最后 `<-done` 等上报结束。采样函数退出前也会检查取消，因此不会在停止后提交新结果。

`trigger` 容量为 1，用于合并“请重新探测”的通知；`Mailbox` 容量为 1，用于保留最新结果。这是两个用途不同的 channel。

**替换 `cmd/cnld-agent/main.go` 的完整内容：**

```go
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/0702hjj/Campus-Network-Login-Discovery/internal/probecore"
	"github.com/0702hjj/Campus-Network-Login-Discovery/internal/probemqtt"
)

func main() {
	path := flag.String("config", "configs/lab.yaml", "YAML configuration")
	flag.Parse()
	c, err := loadConfig(*path)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
	tlsConfig, err := probemqtt.TLSConfig(c.MQTT)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
	log.SetPrefix("node=" + c.NodeID + " ")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := newHTTP(c.Probe.Timeout)
	defer client.CloseIdleConnections()
	trigger := make(chan struct{}, 1)
	worker := &probemqtt.Worker{Mailbox: probemqtt.NewMailbox(), Config: c.MQTT,
		Node: c.NodeID, ValidFor: c.Probe.ValidFor, TLS: tlsConfig, Trigger: trigger}
	done := make(chan struct{})
	go func() { defer close(done); worker.Run(ctx) }()
	sample := func(ctx context.Context) probecore.Result {
		at := time.Now()
		probeCtx, cancel := context.WithTimeout(ctx, c.Probe.Timeout)
		defer cancel()
		return probecore.Classify(observe(probeCtx, client, c.Probe.Endpoint), c.Probe.Rules, at)
	}
	runProbe(ctx, c.Probe.Interval, trigger, sample, worker)
	<-done // 主 goroutine 等待 MQTT 的退出清理
}
```

```sh
gofmt -w cmd internal
go mod tidy
go test ./...
go build -o bin/cnld-agent ./cmd/cnld-agent
./bin/cnld-agent -config configs/lab.yaml
```

**本节完成标志：** 终端 C 看到 availability online 和 report；报告包含连接 ID、递增序号、时间、有效期及两个标签。Broker 停止后，Agent 的 probe 日志仍每 3 秒出现。

## 23 · 合并报告和 availability，得到当前视图（约 10 分钟）

Broker 保存的 `authenticated=true` 可能是十分钟前的结果。消费者要先确认 online、连接 ID 相同、报告未过期，才展示当前标签。

把合并逻辑也写成普通函数。`accept` 只接收更大的序号；相同序号属于重复投递。这里缓存最近收到的最多两个连接的数据，兼容 report 比 availability 先到的情况；多余记录被丢弃后先显示 unknown，等待新的周期报告。

演示消费者接受的 TTL 上限是一天，用于限制时间计算范围；本实验配置为 10 秒。

**新建 `cmd/cnld-watch/state.go`，写入完整内容：**

```go
package main

import (
	"fmt"
	"time"

	"github.com/0702hjj/Campus-Network-Login-Discovery/internal/probemqtt"
)

type view struct {
	availability probemqtt.Availability
	reports      map[string]probemqtt.Report
}

func newView() *view { return &view{reports: make(map[string]probemqtt.Report)} }

func (v *view) accept(r probemqtt.Report) {
	old, ok := v.reports[r.ConnectionID]
	if ok && r.Sequence <= old.Sequence {
		return
	}
	if !ok && len(v.reports) >= 2 {
		v.reports = make(map[string]probemqtt.Report)
	}
	v.reports[r.ConnectionID] = r
}

func (v *view) current(connected bool, node string, now time.Time) string {
	a := v.availability
	if !connected || a.NodeID != node || a.SchemaVersion != 1 || a.ConnectionID == "" {
		return "unknown"
	}
	if a.Status == "offline" {
		return "offline"
	}
	if a.Status != "online" {
		return "unknown"
	}
	r, ok := v.reports[a.ConnectionID]
	if !ok || r.NodeID != node || r.SchemaVersion != 1 || r.Sequence == 0 || r.ValidForSeconds <= 0 ||
		r.ValidForSeconds > 86400 || r.ObservedAt.After(now) ||
		!now.Before(r.ObservedAt.Add(time.Duration(r.ValidForSeconds)*time.Second)) {
		return "unknown"
	}
	auth, ext := r.Labels["network.campus/authenticated"], r.Labels["network.campus/external_access"]
	if auth != "true" && auth != "false" && auth != "unknown" {
		return "unknown"
	}
	if ext != "reachable" && ext != "blocked" && ext != "unknown" {
		return "unknown"
	}
	return fmt.Sprintf("auth=%s external=%s reason=%s", auth, ext, r.Reason)
}
```

```sh
go test ./cmd/cnld-watch
```

**本节完成标志：** 看到 `no test files` 或编译成功即可；这个目录的 main 函数在第 25 节加入。现在不要 `go build ./...`。

## 24 · 测试重复、乱序、过期和断连（约 10 分钟）

测试分别模拟 report 先到和 availability 先到。两种情况都应先 unknown，再得到相同的有效结果。

`now.Add(10*time.Second)` 直接把测试时间推进 10 秒，不需要真的等待。这个办法也是 core 不读系统时钟的价值。

**新建 `cmd/cnld-watch/state_test.go`，写入完整内容：**

```go
package main

import (
	"testing"
	"time"

	"github.com/0702hjj/Campus-Network-Login-Discovery/internal/probemqtt"
)

func TestView(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	report := probemqtt.Report{SchemaVersion: 1, NodeID: "lab-01", ConnectionID: "new", Sequence: 2,
		ObservedAt: now, ValidForSeconds: 10, Labels: map[string]string{
			"network.campus/authenticated": "true", "network.campus/external_access": "unknown"}, Reason: "login_ok"}
	online := probemqtt.Availability{SchemaVersion: 1, NodeID: "lab-01", ConnectionID: "new", Status: "online"}
	for _, reportFirst := range []bool{true, false} {
		v := newView()
		if reportFirst {
			v.accept(report)
		} else {
			v.availability = online
		}
		if v.current(true, "lab-01", now) != "unknown" {
			t.Fatal("incomplete state")
		}
		v.accept(report)
		v.availability = online
		want := "auth=true external=unknown reason=login_ok"
		if got := v.current(true, "lab-01", now); got != want {
			t.Fatal(got)
		}
		duplicate := report
		duplicate.Sequence = 1
		duplicate.Reason = "old"
		v.accept(duplicate)
		v.accept(report)
		if v.current(true, "lab-01", now) != want {
			t.Fatal("duplicate or old sequence replaced latest")
		}
		if v.current(true, "lab-01", now.Add(10*time.Second)) != "unknown" {
			t.Fatal("TTL")
		}
		if v.current(false, "lab-01", now) != "unknown" {
			t.Fatal("consumer disconnected")
		}
		v.availability.ConnectionID = "other"
		if v.current(true, "lab-01", now) != "unknown" {
			t.Fatal("ID mismatch")
		}
		v.availability = online
		v.availability.Status = "offline"
		if v.current(true, "lab-01", now) != "offline" {
			t.Fatal("offline")
		}
	}
}
```

```sh
go test -v ./cmd/cnld-watch
```

**本节完成标志：** 重复、较旧序号、跨 Topic 两种到达次序、TTL 边界、连接 ID 不匹配、消费者失联及 offline 全部被检查。

## 25 · 写一个本机订阅工具，定时检查 TTL（约 10 分钟）

MQTT 回调把消息复制进 channel，主循环解析和更新视图。即使没有新消息，1 秒定时器也会检查过期。

这个辅助工具每次重连重新订阅并清空视图；连接失败时显示 unknown。它的用途是本机实验。第 30 节演示远程 TLS 时使用支持 CA 参数的 `mosquitto_sub`。

多个订阅工具使用不同进程 PID 的 Client ID，不会与 Agent 相互踢线。

**新建 `cmd/cnld-watch/main.go`，写入完整内容：**

```go
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/0702hjj/Campus-Network-Login-Discovery/internal/probemqtt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type event struct {
	topic string
	body  []byte
}

func main() {
	node := flag.String("node", "lab-01", "node ID")
	broker := flag.String("broker", "tcp://127.0.0.1:1883", "lab broker")
	prefix := flag.String("prefix", "cnld/nodes", "topic prefix")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// 本辅助程序用于本机实验；远程 TLS 订阅可用后文的 mosquitto_sub。
	for ctx.Err() == nil {
		watch(ctx, *broker, *prefix, *node)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func watch(ctx context.Context, broker, prefix, node string) {
	events := make(chan event, 16)
	lost := make(chan struct{}, 1)
	notifyLost := func() {
		select {
		case lost <- struct{}{}:
		default:
		}
	}
	options := mqtt.NewClientOptions().AddBroker(broker).
		SetClientID(fmt.Sprintf("cnld-watch-%d", os.Getpid())).SetProtocolVersion(4).
		SetCleanSession(true).SetAutoReconnect(false).SetConnectRetry(false).
		SetConnectTimeout(2 * time.Second).SetKeepAlive(4 * time.Second).SetPingTimeout(time.Second)
	options.SetConnectionLostHandler(func(mqtt.Client, error) { notifyLost() })
	client := mqtt.NewClient(options)
	defer client.Disconnect(100)
	token := client.Connect()
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		log.Print("unknown: connect failed")
		return
	}
	topic := prefix + "/" + node + "/"
	handler := func(_ mqtt.Client, m mqtt.Message) {
		e := event{m.Topic(), append([]byte(nil), m.Payload()...)}
		select {
		case events <- e:
		default:
			notifyLost()
		} // 队列满则重新订阅，重新建立可信状态
	}
	token = client.SubscribeMultiple(map[string]byte{topic + "report": 1, topic + "availability": 1}, handler)
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		log.Print("unknown: subscribe failed")
		return
	}
	state := newView() // 每次重连都清空本地视图
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	previous := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-lost:
			log.Print("unknown: subscription lost")
			return
		case e := <-events:
			if strings.HasSuffix(e.topic, "/report") {
				var r probemqtt.Report
				if json.Unmarshal(e.body, &r) == nil {
					state.accept(r)
				}
			} else {
				var a probemqtt.Availability
				if json.Unmarshal(e.body, &a) == nil {
					state.availability = a
				}
			}
		case <-ticker.C:
			value := state.current(client.IsConnectionOpen(), node, time.Now())
			if value != previous {
				log.Print(value)
				previous = value
			}
		}
	}
}
```

```sh
go build -o bin/cnld-watch ./cmd/cnld-watch
./bin/cnld-watch -node lab-01
```

**本节完成标志：** Agent 在线时看到 `auth=true external=unknown reason=login_ok`。停止 Broker 后显示 unknown；Broker 恢复后能重新订阅。

## 26 · 验证认证变化与错误分类（约 10 分钟）

保持假接口、Broker、Agent、watch 四个进程运行。每改一种模式，等下一次 Agent 日志和 report，再改下一种。实验周期是 3 秒。

```sh
printf yes > /tmp/cnld-mode
# 等下一轮，再执行下面一行
printf no > /tmp/cnld-mode
# 再等下一轮
printf portal > /tmp/cnld-mode
# 按相同方式继续
printf redirect > /tmp/cnld-mode
printf slow > /tmp/cnld-mode
printf large > /tmp/cnld-mode
printf unexpected > /tmp/cnld-mode
printf yes > /tmp/cnld-mode
```

逐项对照，不要一次把所有命令执行完：

| 模式 | authenticated | reason |
| --- | --- | --- |
| yes | true | login_ok |
| no | false | explicit_unauthenticated |
| portal | false | known_portal |
| redirect | unknown | unrecognized_response |
| slow | unknown | timeout |
| large | unknown | body_too_large |
| unexpected | unknown | unrecognized_response |

最后在终端 A 按 Ctrl+C 停止假接口。Agent 应继续运行并报告 `unknown / connection_error`，重启假接口后恢复。实验中 `external_access` 一直为 `unknown`。

**本节完成标志：** 普通跳转、超时和连接失败都没有被当成明确未认证。状态没变化时，sequence 和 observed_at 也继续更新。

## 27 · 比较正常退出和崩溃退出（约 10 分钟）

先在前台 Agent 中按 Ctrl+C。订阅者应看到 `offline / graceful_shutdown`。重新启动时记下新的 `connection_id`。

强杀实验用编译后的二进制，才能准确找到真正的 Agent PID。停止已有 Agent，然后执行：

```sh
./bin/cnld-agent -config configs/lab.yaml > /tmp/cnld-agent.log 2>&1 &
agent_pid=$!
printf '%s
' "$agent_pid"
```

等 report 出现，记下连接 ID，再执行：

```sh
kill -KILL "$agent_pid"
mosquitto_sub -h 127.0.0.1 -q 1 -t cnld/nodes/lab-01/availability -v
```

等待 Broker 检测断连，新订阅者也应读到 retained `offline / connection_lost`。接下来启动 Agent，online 和 report 应使用另一个连接 ID。

本实验 Keep Alive=4s。Broker 检测依赖协议心跳和实际连接状况，观察日志记录检测过程，不把它当作固定的告警秒表。

**本节完成标志：** 两种退出都显示 offline，reason 不同；认证历史没有被改写为 false。

## 28 · 亲眼看到“报告过期，但连接还没判离线”（约 10 分钟）

先停止已有 Agent。复制配置，把 Keep Alive 调长，使报告过期先于 Broker 心跳失效：

```sh
sed 's/keep_alive: 4s/keep_alive: 60s/' configs/lab.yaml > configs/ttl.yaml
./bin/cnld-agent -config configs/ttl.yaml > /tmp/cnld-agent.log 2>&1 &
agent_pid=$!
```

等 watch 显示有效的 auth 后，执行：

```sh
kill -STOP "$agent_pid"
```

SIGSTOP 暂停整个进程。约 10 秒后，报告过期，watch 先显示 unknown；此时 Broker 可能还保存 online。继续观察会因心跳失效得到 offline。

恢复并结束进程：

```sh
kill -CONT "$agent_pid"
kill -TERM "$agent_pid"
wait "$agent_pid"
```

**本节完成标志：** 没有新消息时，watch 自己发现 TTL 过期。TTL 能发现停滞的采样；LWT 描述连接状态，二者负责不同问题。

## 29 · 用一个丢包代理测试 MQTT 故障隔离（约 10 分钟）

我们只让 Agent 的 MQTT 流量通过 1884 端口。watch 仍直连 1883，HTTP 仍直连 18080，这样能单独观察故障。

这个实验工具在存在 `/tmp/cnld-blackhole` 文件时读走并丢弃两个方向的数据。它保持 TCP socket，模拟 MQTT 控制报文无法到达的情况。两个 `pipe` goroutine 分别处理两个方向。

**新建 `cmd/fake-link/main.go`，写入完整内容：**

```go
package main

import (
	"log"
	"net"
	"os"
)

func pipe(dst, src net.Conn) {
	defer dst.Close()
	defer src.Close()
	buffer := make([]byte, 4096)
	for {
		n, err := src.Read(buffer)
		if err != nil {
			return
		}
		if _, err := os.Stat("/tmp/cnld-blackhole"); err == nil {
			continue
		}
		// Write 可能只发送部分字节，所以一直写到完成。
		data := buffer[:n]
		for len(data) > 0 {
			n, err := dst.Write(data)
			if err != nil {
				return
			}
			data = data[n:]
		}
	}
}

func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:1884")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	for {
		agent, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		broker, err := net.Dial("tcp", "127.0.0.1:1883")
		if err != nil {
			agent.Close()
			continue
		}
		go pipe(agent, broker)
		go pipe(broker, agent)
	}
}
```

在额外的终端启动代理：

```sh
go run ./cmd/fake-link
```

停止已有 Agent，再执行：

```sh
sed 's/:1883/:1884/' configs/lab.yaml > configs/blackhole.yaml
./bin/cnld-agent -config configs/blackhole.yaml > /tmp/cnld-agent.log 2>&1 &
agent_pid=$!
tail -f /tmp/cnld-agent.log
```

等 online/report 出现，在另一个终端执行：

```sh
touch /tmp/cnld-blackhole
```

观察 Agent 仍每 3 秒打印 probe；MQTT 超时并退避。Broker 检测心跳失效后，直连的 watch 收到 offline。

接着恢复链路：

```sh
rm /tmp/cnld-blackhole
```

等重连，检查新连接 ID 与新采样时间，再结束此实验的 Agent 和代理。

**本节完成标志：** Broker 故障没有阻止 HTTP 探测。恢复后没有回放一串历史报告；第一份有效报告来自新连接建立之后的采样。

## 30 · 让本机实验先跑通 TLS（约 10 分钟）

先生成一套只用于实验、有效期 7 天的 CA 与服务端证书。证书的 SAN 包含 localhost 和 127.0.0.1，使客户端能检查服务器身份。

新建 `deploy/certs/`；下面命令在仓库根目录执行：

```sh
mkdir -p deploy/certs
openssl req -x509 -newkey rsa:2048 -nodes   -keyout deploy/certs/lab-ca.key -out deploy/certs/lab-ca.crt   -days 7 -subj '/CN=CNLD Lab CA'
openssl req -newkey rsa:2048 -nodes   -keyout deploy/certs/broker.key -out deploy/certs/broker.csr   -subj '/CN=localhost'
printf '%s
' 'subjectAltName=DNS:localhost,IP:127.0.0.1'   'extendedKeyUsage=serverAuth' > deploy/certs/broker.ext
openssl x509 -req -in deploy/certs/broker.csr   -CA deploy/certs/lab-ca.crt -CAkey deploy/certs/lab-ca.key -CAcreateserial   -out deploy/certs/broker.crt -days 7 -extfile deploy/certs/broker.ext
```

生成 `deploy/mosquitto-tls.conf`，其中证书路径写为绝对路径：

```sh
cat > deploy/mosquitto-tls.conf <<EOF
listener 8883 127.0.0.1
allow_anonymous true
cafile $(pwd)/deploy/certs/lab-ca.crt
certfile $(pwd)/deploy/certs/broker.crt
keyfile $(pwd)/deploy/certs/broker.key
EOF
```

在额外的终端启动 TLS Broker：

```sh
mosquitto -c deploy/mosquitto-tls.conf -v
```

生成 TLS Agent 配置：

```sh
sed 's#tcp://127.0.0.1:1883#tls://localhost:8883#' configs/lab.yaml > configs/tls.yaml
printf '  ca_file: deploy/certs/lab-ca.crt
' >> configs/tls.yaml
./bin/cnld-agent -config configs/tls.yaml
```

在另一个终端订阅：

```sh
mosquitto_sub -h localhost -p 8883 --cafile deploy/certs/lab-ca.crt -q 1 -t 'cnld/nodes/lab-01/#' -v
```

**本节完成标志：** TLS 连接能上报。临时去掉 Agent 的 ca_file，用系统不认识的实验 CA 会连接失败；恢复后成功。`cert_file` / `key_file` 可用于后续要求客户端证书的 Broker。

## 31 · 添加用户、Topic 权限和 retained 持久化（约 10 分钟）

TLS 解决“发给谁、传输加密”；用户名和 ACL 决定“谁能发到哪个 Topic”。本节继续使用 8883 的 TLS Broker，先停止它和 TLS Agent。

创建两个用户，命令会交互询问密码：

```sh
mosquitto_passwd -c deploy/passwords agent-lab
mosquitto_passwd deploy/passwords observer
mkdir -p deploy/broker-data
```

新建 `deploy/acl`：

```text
user agent-lab
topic write cnld/nodes/lab-01/report
topic write cnld/nodes/lab-01/availability

user observer
topic read cnld/nodes/+/report
topic read cnld/nodes/+/availability
```

替换 TLS Broker 配置：

```sh
cat > deploy/mosquitto-tls.conf <<EOF
listener 8883 127.0.0.1
allow_anonymous false
password_file $(pwd)/deploy/passwords
acl_file $(pwd)/deploy/acl
cafile $(pwd)/deploy/certs/lab-ca.crt
certfile $(pwd)/deploy/certs/broker.crt
keyfile $(pwd)/deploy/certs/broker.key
persistence true
persistence_location $(pwd)/deploy/broker-data/
autosave_interval 5
EOF
chmod 600 deploy/passwords
```

在 `configs/tls.yaml` 的 mqtt 段末尾追加（只追加一次）：

```yaml
username_env: CNLD_MQTT_USERNAME
  password_env: CNLD_MQTT_PASSWORD
```

重新启动 TLS Broker。在运行 Agent 的终端读取用户密码：

```sh
export CNLD_MQTT_USERNAME=agent-lab
read -r -s CNLD_MQTT_PASSWORD
export CNLD_MQTT_PASSWORD
./bin/cnld-agent -config configs/tls.yaml
```

在订阅终端读取 observer 的密码，再运行：

```sh
read -r -s OBSERVER_PASSWORD
mosquitto_sub -h localhost -p 8883 --cafile deploy/certs/lab-ca.crt   -u observer -P "$OBSERVER_PASSWORD" -q 1 -t 'cnld/nodes/lab-01/#' -v
```

等收到报告后，正常停止 Agent 和 Broker，再启动 Broker。新订阅者应读到最后的 report 和 offline。可以用 `mosquitto_pub` 验证 agent-lab 向别的节点 Topic 发布会被拒绝；MQTT 3.1.1 不提供详细的发布权限错误，结合 Broker 日志及消费者是否收到判断。Broker 突然掉电时，保留数据取决于落盘策略；重启后仍按连接 ID 与 TTL 重新验证状态。

将下面模式加入 `.gitignore`，实际证书和凭证保留在实验机：

```gitignore
/bin/
/deploy/certs/
/deploy/passwords
/deploy/broker-data/
/deploy/mosquitto-tls.conf
/configs/tls.yaml
```

**本节完成标志：** Agent 可以向自身两个 Topic 发布，包括 Will；observer 可以订阅；新订阅者能读到持久化数据。生产部署使用实际 Broker 证书和地址，不使用这套 7 天实验 CA。配置依据：[Mosquitto 配置手册](https://mosquitto.org/man/mosquitto-conf-5.html)。

## 32 · 编译二进制，让 systemd 管理它（约 10 分钟）

先把实验做成普通 Linux 程序。下面用用户级 systemd 服务，配置路径都在当前用户目录。先停止其他同 node_id 的 Agent。

编译并安装到用户目录：

```sh
go build -trimpath -o bin/cnld-agent ./cmd/cnld-agent
mkdir -p "$HOME/.local/bin" "$HOME/.config/cnld" "$HOME/.config/systemd/user"
cp bin/cnld-agent "$HOME/.local/bin/"
cp configs/lab.yaml "$HOME/.config/cnld/config.yaml"
```

新建 `~/.config/systemd/user/cnld-agent.service`。`%h` 由 systemd 展开为用户目录，不是 Go 语法：

```ini
[Unit]
Description=Campus network discovery agent

[Service]
ExecStart=%h/.local/bin/cnld-agent -config %h/.config/cnld/config.yaml
WorkingDirectory=%h/.config/cnld
EnvironmentFile=-%h/.config/cnld/credentials.env
Restart=on-failure
RestartSec=3
TimeoutStopSec=10

[Install]
WantedBy=default.target
```

```sh
systemctl --user daemon-reload
systemctl --user enable --now cnld-agent
journalctl --user -u cnld-agent -f
```

本节仍需本机假接口和 1883 Broker。切换 TLS 配置时，`ca_file` 等文件路径改为绝对路径；凭证文件可写 `CNLD_MQTT_USERNAME=...` 与 `CNLD_MQTT_PASSWORD=...` 两行，再 `chmod 600 ~/.config/cnld/credentials.env`。

停止服务，订阅者应得到正常 offline：

```sh
systemctl --user stop cnld-agent
```

**本节完成标志：** 能通过 systemd 启动、看日志、停止。用户服务默认与登录会话有关；需要退出登录后继续运行时，再按实验机管理方式启用该用户的 linger，或改为系统级服务。

## 33 · 交付 amd64、arm64 二进制与 OCI 镜像（约 10 分钟）

先完成交叉编译。我们的 Agent 不需要 C 动态库，因此使用 `CGO_ENABLED=0`：

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o bin/cnld-agent-linux-amd64 ./cmd/cnld-agent
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -o bin/cnld-agent-linux-arm64 ./cmd/cnld-agent
file bin/cnld-agent-linux-*
```

新建 `Dockerfile`：

```dockerfile
FROM golang:alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -trimpath -o /cnld-agent ./cmd/cnld-agent

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /cnld-agent /cnld-agent
USER 65532:65532
ENTRYPOINT ["/cnld-agent"]
CMD ["-config", "/config.yaml"]
```

新建 `.dockerignore`，避免构建上下文带入实验私钥：

```text
.git
bin
deploy
configs
*.md
```

构建并在 Linux 上运行。先停止其他 Agent：

```sh
docker build -t cnld:lab .
docker run --rm --name cnld-lab --network host   -v "$PWD/configs/lab.yaml:/config.yaml:ro" cnld:lab
```

这里 host networking 使容器使用 Linux 宿主机出口；本机 18080/1883 也因此可达。再开一个终端执行 `docker stop cnld-lab`，检查 graceful offline。远程 TLS 时额外挂载 CA 文件，并使用 `--env-file` 提供凭证。

需要同时导出两种架构的 OCI 镜像时使用：

```sh
docker buildx build --platform linux/amd64,linux/arm64   --output type=oci,dest=bin/cnld-images.tar .
```

**本节完成标志：** 得到两个 Linux 二进制、一个可运行镜像，以及双架构 OCI 导出物。记录本次构建使用的 Go 镜像 digest；之后可把 Dockerfile 的基础镜像固定到这个 digest。

## 34 · 把 MQTT 生命周期实验变成自动测试（约 10 分钟）

前面手工观察已经能定位问题。现在保留一份能重复运行的集成测试：它启动临时 Broker 和假接口，编译并启动 Agent，然后订阅 JSON 检查字段。

测试默认跳过，只有显式设置 `CNLD_INTEGRATION=1` 才运行。开始前停止本书其他实验进程，释放 1883 和 18080 端口；Broker 使用临时配置和目录。这个测试运行在 Linux，使用前面安装的 Mosquitto 工具。

`exec.Command` 对应“启动另一个进程”，`t.Cleanup` 对应“测试结束时收回子进程”。`await` 最多等 12 秒，并用真正的 MQTT 消息判断成功。先读最后的 Test 函数，再看三个辅助函数。

创建目录并写入完整文件：

```sh
mkdir -p tests/integration
```

**新建 `tests/integration/mqtt_test.go`，写入完整内容：**

```go
package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/0702hjj/Campus-Network-Login-Discovery/internal/probemqtt"
)

func start(t *testing.T, dir, name string, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	})
	return cmd
}

func waitPort(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("listener did not start: %s", address)
}

func await(t *testing.T, topic string, match func([]byte) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "mosquitto_sub", "-h", "127.0.0.1", "-q", "1", "-t", topic)
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		if match(scanner.Bytes()) {
			return
		}
	}
	t.Fatalf("no matching message on %s: %v", topic, scanner.Err())
}

func TestMQTTLifecycle(t *testing.T) {
	if os.Getenv("CNLD_INTEGRATION") != "1" {
		t.Skip("set CNLD_INTEGRATION=1")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	temp := t.TempDir()
	for _, tool := range []string{"mosquitto", "mosquitto_sub"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatal(err)
		}
	}
	// 本测试独占两个固定端口，避免把消息送给正在运行的实验服务。
	for _, address := range []string{"127.0.0.1:1883", "127.0.0.1:18080"} {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("stop other lab processes first: %v", err)
		}
		listener.Close()
	}
	for _, name := range []string{"cnld-agent", "fake-campus"} {
		cmd := exec.Command("go", "build", "-o", filepath.Join(temp, name), "./cmd/"+name)
		cmd.Dir = root
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build: %s %v", output, err)
		}
	}
	setMode := func(mode string) {
		t.Helper()
		if err := os.WriteFile("/tmp/cnld-mode", []byte(mode), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { os.Remove("/tmp/cnld-mode") })
	setMode("yes")
	brokerConfig := filepath.Join(temp, "mosquitto.conf")
	if err := os.WriteFile(brokerConfig, []byte("listener 1883 127.0.0.1\nallow_anonymous true\npersistence false\n"), 0600); err != nil {
		t.Fatal(err)
	}
	start(t, root, "mosquitto", "-c", brokerConfig)
	waitPort(t, "127.0.0.1:1883")
	start(t, root, filepath.Join(temp, "fake-campus"))
	waitPort(t, "127.0.0.1:18080")
	launch := func() *exec.Cmd {
		return start(t, root, filepath.Join(temp, "cnld-agent"), "-config", "configs/lab.yaml")
	}
	reportTopic := "cnld/nodes/lab-01/report"
	availabilityTopic := "cnld/nodes/lab-01/availability"
	var current probemqtt.Report
	auth := func(want string, oldID string) {
		await(t, reportTopic, func(body []byte) bool {
			var r probemqtt.Report
			if json.Unmarshal(body, &r) != nil {
				return false
			}
			if r.Labels["network.campus/authenticated"] != want || r.ConnectionID == oldID {
				return false
			}
			current = r
			return true
		})
	}
	offline := func(reason string) {
		await(t, availabilityTopic, func(body []byte) bool {
			var a probemqtt.Availability
			return json.Unmarshal(body, &a) == nil && a.ConnectionID == current.ConnectionID &&
				a.Status == "offline" && a.Reason == reason
		})
	}
	agent := launch()
	auth("true", "")
	setMode("no")
	auth("false", "")
	setMode("slow")
	auth("unknown", "")
	if err := agent.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := agent.Wait(); err != nil {
		t.Fatal(err)
	}
	offline("graceful_shutdown")
	oldID := current.ConnectionID
	setMode("yes")
	agent = launch()
	auth("true", oldID)
	if err := agent.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	agent.Wait() // 被 SIGKILL 结束时，非零退出是预期结果
	offline("connection_lost")
}
```

```sh
gofmt -w tests/integration
go test ./tests/integration
CNLD_INTEGRATION=1 go test -v -count=1 -timeout 90s ./tests/integration
```

**本节完成标志：** 第一条测试显示 SKIP；显式运行时 PASS，覆盖认证变化、超时、正常退出、强杀遗嘱和重启新连接 ID。它与第 24 节的纯状态测试共同覆盖协议与消费逻辑；第 28–31 节保留为链路、TLS 与 Broker 配置实验。

## 35 · 换成真实接口，完成本次实验的验收（约 10 分钟）

先用 `curl` 记录你的真实校园网接口在已认证、未认证时的状态码、body 和 Location，再把实际规则填进配置。**没有真实响应样本时，先保留本机模拟配置，不猜一套认证规则。**

例如实际接口是下面这两种响应：

```text
HTTP/1.1 200 OK

login_ok

HTTP/1.1 302 Found
Location: http://login.campus.example/login
```

相应规则示例：

```yaml
probe:
  endpoint: http://auth.campus.example/check
  interval: 30s
  timeout: 5s
  valid_for: 90s
  authenticated_rule:
    status_code: 200
    body_equals: login_ok
  unauthenticated_rule:
    status_code: 401
  portal_hosts: [login.campus.example]
```

`body_equals` 比较完整字符串。如果真实接口返回 `login_ok` 后还有换行，配置应写 `body_equals: "login_ok\n"`。这是示例接口，地址与规则需要替换成你的实际值；保留配置其余部分并设置真实稳定的 node_id 和 Broker。

完成以下检查，并把结果记在你自己的实验记录里：

```sh
gofmt -w cmd internal
go test ./...
go test -race ./...
go vet ./...
go list -deps ./cmd/cnld-agent > /tmp/cnld-deps.txt
```

打开 `/tmp/cnld-deps.txt` 搜索 `k8s.io/`，默认实现应没有相关依赖。

| 要验证的行为 | 对应步骤 / 证据 |
| --- | --- |
| 无 K8s 环境可独立运行 | 22，默认依赖图 |
| core 不访问网络与时钟 | 02–05，固定时间测试 |
| 已认证、明确未认证、未知不混淆 | 26，状态和 reason |
| 新订阅者得到 retained 状态 | 13、22、27 |
| 正常退出和强杀都能显示 offline | 27，两个 reason |
| 断网探测继续，恢复后新 ID + 新采样 | 29，probe 日志和报告 |
| 重复、乱序、过期、连接不匹配不误用 | 24，纯状态测试 |
| 心跳未过期时报告可以先过期 | 28，watch 从标签转 unknown |
| Broker 重启 / 订阅者断网后重新建立视图 | 25、31，重连日志 |
| TLS、凭证与 Topic 权限 | 30–31，成功与拒绝日志 |
| 两种架构二进制和镜像 | 33，构建产物 |

再给 MQTT 集成实验补一条**离线发布失败**验证：Agent 通过 29 的丢包代理连接；确认 online 后创建黑洞文件，随即向 Agent 发 SIGTERM。Agent 日志应出现 `offline not confirmed`，随后关闭 socket；Broker 可能已因心跳发布 Will，或稍后发布。订阅者得到 offline 后再删除黑洞文件。这个顺序走到 `shutdown` 的失败分支，没有用正常 DISCONNECT 取消遗嘱。

建议用第 29 节的固定场景逐渐写成 `tests/integration/` 下的脚本：启动 Broker → 启动假接口 → 启动 Agent → 用带超时的订阅检查 JSON → 结束所有子进程。每个检查比较字段值，不只搜索日志里的 PASS。

完成本次独立版之后，可以按这个顺序继续练习：

1. 增加独立外网 Observation，仍由 core 做纯判定；不要从 authenticated 推导 external_access。
2. 若需要单次探测，把第 09 节入口整理成 `probe` 子命令。
3. 需要 K8s 时才创建 `internal/probek8s` 和 `k8s` build tag 入口，另开一个独立 sink worker；仍只 patch 托管键。先设计掉线标签如何失效，再编写 DaemonSet。

### 按问题查参考资料

| 你正在处理的问题 | 原始参考 |
| --- | --- |
| module、package 和运行命令 | [Go 入门](https://go.dev/doc/tutorial/getting-started)、[目录布局](https://go.dev/doc/modules/layout) |
| Go 接口和方法 | [Effective Go](https://go.dev/doc/effective_go#interfaces_and_types) |
| HTTP 取消、超时和重定向 | [net/http](https://pkg.go.dev/net/http)、[context](https://pkg.go.dev/context) |
| channel 与 goroutine | [Go Tour：并发](https://go.dev/tour/concurrency/1) |
| YAML duration、已知字段检查 | [YAML v3](https://pkg.go.dev/go.yaml.in/yaml/v3) |
| MQTT 客户端 API | [Paho v1.5.1](https://pkg.go.dev/github.com/eclipse/paho.mqtt.golang@v1.5.1) |
| Will、QoS、retained、Keep Alive | [MQTT 3.1.1 §3.1.2、§3.3、§3.14、§4.3](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) |
| Broker 配置与订阅参数 | [Mosquitto 配置](https://mosquitto.org/man/mosquitto-conf-5.html)、[mosquitto_sub](https://mosquitto.org/man/mosquitto_sub-1.html) |

### 参考代码验证记录

本文中的 Agent、假接口、假链路与本机订阅工具曾从代码块拼装到临时 Go module 中验证。普通测试、race 检查和 vet 通过；使用真实 Mosquitto 验证了认证变化、正常退出、强杀遗嘱、双向丢包和新连接恢复。TLS CA、用户名密码、Topic ACL、错误 CA 拒绝和 Broker 重启后的 retained 持久化也已验证。第 34 节的自动集成测试已通过，Linux amd64/arm64 二进制交叉编译也已通过。systemd、Docker 运行与双架构 OCI 导出尚未在当前环境执行；这些步骤由你在实验机上完成。

**本节完成标志：** 能从一次观测值解释标签为什么变化，并能用订阅结果说明它何时失去可信度。这比只看到进程一直在运行更能说明实验完成。

