package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/autopaho/queue/file"
	"github.com/eclipse/paho.golang/paho"
	"github.com/fsnotify/fsnotify"
	"github.com/justinas/alice"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/client"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/hlog"
)

type monitoredContainers struct {
	nameToId map[string]string
	mu       sync.RWMutex
}

func (mc *monitoredContainers) add(name string, id string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.nameToId[name] = id
}

func (mc *monitoredContainers) del(name string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	delete(mc.nameToId, name)
}

func (mc *monitoredContainers) isMonitored(name string) bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if _, ok := mc.nameToId[name]; ok {
		return true
	}

	return false
}

type purgeMessage struct {
	URL           *url.URL
	Header        http.Header
	Sender        string
	ContainerName string
	ClientIP      netip.Addr
	ClientTLS     bool
}

// Headers that are never propagated to other nodes. Credentials and
// per-client session state have no place in a cache purge, and
// forwarding them would store them in the MQTT queue and broker.
// Hop-by-hop headers describe the original connection, not the request.
// Keys must be in canonical form (http.CanonicalHeaderKey).
//
// Headers nominated by a Connection header are deliberately propagated.
// Vinyl keeps them visible to VCL and only filters them when forwarding
// to a backend, so removing them would make peers evaluate a different
// request than the original node. Honoring Connection here would also let
// the sender strip arbitrary headers, such as a purge key, from the
// propagated copy
var unpropagatedHeaders = map[string]bool{
	"Authorization":       true,
	"Proxy-Authorization": true,
	"Cookie":              true,
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Connection":    true,
	"Transfer-Encoding":   true,
	"Te":                  true,
	"Upgrade":             true,
	"Content-Length":      true,
	"Trailer":             true,
}

// stripLastListEntry removes the entry Vinyl's core appended to a
// list-valued header, either at the end of the last line or as a line
// of its own. The receiving node's core appends the same entry again,
// so without this X-Forwarded-For would contain the client IP twice.
func stripLastListEntry(h http.Header, key string) {
	key = http.CanonicalHeaderKey(key)
	if len(h[key]) == 0 {
		return
	}
	last := len(h[key]) - 1
	line := h[key][last]

	// Appended to an existing line: "a, b" -> "a"
	if i := strings.LastIndex(line, ","); i >= 0 {
		h[key][last] = strings.TrimSpace(line[:i])
		return
	}

	// A line of its own: drop the line.
	h[key] = h[key][:last]

	// If this was the only line, drop the key entirely.
	if len(h[key]) == 0 {
		h.Del(key)
	}
}

// removeHeaderValue removes one line with exactly this value, applying
// a single ReqUnset entry from vinyllog.
func removeHeaderValue(h http.Header, key, value string) {
	key = http.CanonicalHeaderKey(key)
	i := slices.Index(h[key], value)
	if i < 0 {
		return
	}
	h[key] = slices.Delete(h[key], i, i+1)
	if len(h[key]) == 0 {
		h.Del(key)
	}
}

// hostSocketPath maps socketPath, a path inside the named container, to
// the corresponding path on the host using the container's mounts.
func hostSocketPath(ctx context.Context, dockerClient *client.Client, containerName string, socketPath string) (string, error) {
	res, err := dockerClient.ContainerInspect(ctx, containerName, client.ContainerInspectOptions{})
	if err != nil {
		return "", fmt.Errorf("unable to inspect container %q: %w", containerName, err)
	}

	var hostPath string
	bestMatchLength := -1
	for _, m := range res.Container.Mounts {
		destination := filepath.Clean(m.Destination)
		rel, err := filepath.Rel(destination, socketPath)
		if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
			continue
		}
		if len(destination) > bestMatchLength {
			hostPath = filepath.Join(m.Source, rel)
			bestMatchLength = len(destination)
		}
	}
	if bestMatchLength >= 0 {
		return hostPath, nil
	}

	return "", fmt.Errorf("no mount in container %q contains %s", containerName, socketPath)
}

// dialUnixLongPath connects to a Unix socket whose path may exceed the
// 107-character sun_path limit. It opens the socket's directory and
// connects via /proc/self/fd/<fd>/<name>, a short path to the same file.
// Linux-only.
func dialUnixLongPath(ctx context.Context, logger zerolog.Logger, path string) (net.Conn, error) {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("unable to open socket directory: %w", err)
	}
	defer func() {
		if cErr := dir.Close(); cErr != nil {
			logger.Err(cErr).Msg("unable to close unix dial dir")
		}
	}()

	shortPath := fmt.Sprintf("/proc/self/fd/%d/%s", dir.Fd(), filepath.Base(path))

	var d net.Dialer
	return d.DialContext(ctx, "unix", shortPath)
}

// Headers Vinyl's core appends its own entry to before vcl_recv. The
// receiving node appends its own entry again, so the last entry is
// stripped before publishing.
var coreAppendedHeaders = []string{"X-Forwarded-For", "Via"}

const purgerOriginHeader = "Sunet-Cdn-Purger-Origin"

// Name of the vinyl listener the purger sends purges to. Must match the
// name in vinyl's -a argument, e.g. -a purger=/purger/unix-sockets/vinyl
const purgerListenerName = "purger"

func messagePublisher(ctx context.Context, wg *sync.WaitGroup, payloadChan chan []byte, logger zerolog.Logger, cm *autopaho.ConnectionManager, pubTopic string) {
	defer wg.Done()

	for payload := range payloadChan {
		if err := cm.PublishViaQueue(ctx, &autopaho.QueuePublish{
			Publish: &paho.Publish{
				QoS:     1,
				Topic:   pubTopic,
				Payload: payload,
			},
		}); err != nil {
			logger.Error().Err(err).Msg("failed queueing message")
		}

		logger.Info().Str("topic", pubTopic).Msg("added message to queue")

		// The queue relies on the file ModTime to work out what file is oldest; this means the resolution of
		// update times becomes important. To ensure order is maintained add a delay.
		time.Sleep(time.Millisecond)
	}

	logger.Info().Msg("messagePublisher: exiting")
}

func messageSubscriber(ctx context.Context, wg *sync.WaitGroup, msgChan chan *paho.Publish, logger zerolog.Logger, sender string, debug bool, handleLocalMessages bool, socketPath string, dockerClient *client.Client) {
	defer wg.Done()

	var purgerWg sync.WaitGroup

	for msg := range msgChan {
		logger.Info().Str("topic", msg.Topic).Msg("got message from queue")

		pm := purgeMessage{}

		err := json.Unmarshal(msg.Payload, &pm)
		if err != nil {
			logger.Error().Err(err).Msg("unable to parse message")
			continue
		}

		if debug {
			marshalledJson, err := json.MarshalIndent(pm, "", "  ")
			if err != nil {
				logger.Error().Err(err).Msg("unable to parse message JSON")
			}
			fmt.Println("parsed JSON data:")
			fmt.Println(string(marshalledJson))
		}

		// We subscribe with the NoLocal flag so we should not be
		// seeing messages from ourselves, but check the sender field
		// just in case. This can happen e.g. if a previous instance
		// ran with "-danger-handle-local-messages" (meaning NoLocal
		// was false) and the session has not expired yet in the MQTT
		// server (in this case us setting NoLocal to true will have no
		// effect). If this happens you can exit the process and wait
		// for the session to expire before starting again.
		//
		// Since it is helpful to parse our own messages when testing
		// stuff we can skip this check via flag.
		if !handleLocalMessages {
			if pm.Sender == sender {
				logger.Info().Str("msg_sender", pm.Sender).Msg("skipping message sent by myself")
				continue
			}
		}

		purgerWg.Add(1)
		go sendLocalPurge(ctx, &purgerWg, logger, dockerClient, socketPath, pm)
	}

	logger.Info().Msg("messageSubscriber: waiting for outstanding local purge requests")
	purgerWg.Wait()

	logger.Info().Msg("messageSubscriber: exiting")
}

// This is the function that will be called to purge our local cache based on
// the contents of a purge message from another cache node.
func sendLocalPurge(ctx context.Context, wg *sync.WaitGroup, logger zerolog.Logger, dockerClient *client.Client, socketPath string, pm purgeMessage) {
	defer wg.Done()

	if pm.URL == nil {
		logger.Error().Msg("purge message has no URL, skipping")
		return
	}

	req, err := http.NewRequestWithContext(ctx, "PURGE", "http://vinyl"+pm.URL.RequestURI(), nil)
	if err != nil {
		logger.Err(err).Msg("unable to create HTTP request")
		return
	}

	req.Header = pm.Header.Clone()
	if req.Header == nil {
		req.Header = http.Header{}
	}

	// Go's client ignores Host in req.Header, it must be set on req.Host.
	req.Host = pm.Header.Get("Host")
	req.Header.Del("Host")

	// Go sends a default User-Agent when the header is absent. A present
	// but empty entry suppresses it, so absent stays absent.
	if _, ok := req.Header["User-Agent"]; !ok {
		req.Header["User-Agent"] = nil
	}

	// Record which node the purge originated from, so runParser on this
	// node can include it in its log. Loop prevention uses the listener
	// name instead, so this header has no effect on publishing.
	req.Header.Set(purgerOriginHeader, pm.Sender)

	hostPath, err := hostSocketPath(ctx, dockerClient, pm.ContainerName, socketPath)
	if err != nil {
		logger.Error().Err(err).Str("container", pm.ContainerName).Msg("unable to find host vinyl socket for purge")
		return
	}

	purgeClient, err := newPurgeClient(logger, hostPath, pm.ClientIP, pm.ClientTLS)
	if err != nil {
		logger.Err(err).Msg("unable to create purge client")
		return
	}
	// The client is only used for this purge, close its connection when
	// done instead of leaving it idle in the pool.
	defer purgeClient.CloseIdleConnections()

	resp, err := purgeClient.Do(req)
	if err != nil {
		logger.Err(err).Msg("unable to send purge request")
		return
	}
	defer func() {
		if cErr := resp.Body.Close(); cErr != nil {
			logger.Err(cErr).Msg("unable to close local purge body")
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		logger.Error().Int("status_code", resp.StatusCode).Str("body", string(body)).Msg("failed sending local purge")
	}
}

func containerMonitor(ctx context.Context, wg *sync.WaitGroup, msgChan chan []byte, logger zerolog.Logger, sender string, debug bool, dockerClient *client.Client, containerPrefix string, containerQualifier string) {
	defer wg.Done()

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	// Keep track of currently monitored containers in a name -> ID mapping
	mc := &monitoredContainers{
		nameToId: map[string]string{},
	}

monitorLoop:
	for {
		containers, err := dockerClient.ContainerList(context.Background(), client.ContainerListOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("unable to list containers")
			continue
		}

		// Now start a vinyllog parser for each container matching our naming convention.
		for _, ctr := range containers.Items {
			for _, name := range ctr.Names {
				// The docker API returns names with a leading
				// slash ("/"), which is not visible when
				// running `docker ps`, lets trim that
				// here as well to look the same.
				name = strings.TrimPrefix(name, "/")

				if strings.HasPrefix(name, containerPrefix) && strings.Contains(name, containerQualifier) {
					if !mc.isMonitored(name) {
						logger.Info().Str("name", name).Str("id", ctr.ID).Str("image", ctr.Image).Str("status", ctr.Status).Msg("found unmonitored SUNET CDN vinyl container, starting vinyllog")
						mc.add(name, ctr.ID)
						wg.Add(1)
						go vinyllogReader(ctx, name, ctr.ID, wg, msgChan, logger, sender, debug, dockerClient, mc)
					}
				}
			}
		}

		// Wait some time before the next iteration or exit if a signal
		// has been received.
		select {
		case <-ticker.C:
		case <-ctx.Done():
			break monitorLoop
		}
	}
}

// https://stackoverflow.com/questions/52774830/docker-exec-command-from-golang-api
// https://github.com/moby/moby/blob/8e610b2b55bfd1bfa9436ab110d311f5e8a74dcb/integration/internal/container/exec.go#L38
func dockerExec(ctx context.Context, cli client.APIClient, id string, cmd []string, logger zerolog.Logger, debug bool, msgChan chan []byte, sender string, containerName string) error {
	execConfig := client.ExecCreateOptions{
		AttachStdout: true,
		AttachStderr: true,
		Cmd:          cmd,
		// There is no way in the docker exec API to stop a started
		// "exec process":
		// https://github.com/moby/moby/issues/9098
		//
		// We want a way to tell a started process to stop if we
		// are shutting down, otherwise they will be left running in
		// the container forever.
		//
		// Since there is no way to signal the process directly via
		// the container attachement we instead enable a TTY and attach
		// to stdin (equivalent of running "docker exec -it"). This way
		// we can simulate pressing Ctrl+C by sending the equivalent
		// End-of-Text (ETX) byte (0x3) to stdin, causing the TTY to
		// SIGINT the process for us.
		AttachStdin: true,
		TTY:         true,
	}

	cresp, err := cli.ExecCreate(ctx, id, execConfig)
	if err != nil {
		return fmt.Errorf("dockerExec: unable to create exec: %w", err)
	}
	execID := cresp.ID

	// Start the process
	aresp, err := cli.ExecAttach(ctx, execID, client.ExecAttachOptions{})
	if err != nil {
		return fmt.Errorf("dockerExec: unable to attach to exec: %w", err)
	}
	defer aresp.Close()

	// Have the process shut down if needed
	defer func() {
		iresp, err := cli.ExecInspect(context.Background(), execID, client.ExecInspectOptions{})
		if err != nil {
			logger.Error().Err(err).Msg("unable to inspect process in container")
			return
		}
		if !iresp.Running {
			// The process has already exited, no need to simulate Ctrl+C
			return
		}

		// Simulate Ctrl+C
		// https://en.wikipedia.org/wiki/End-of-Text_character
		_, err = aresp.Conn.Write([]byte{0x3})
		if err != nil {
			logger.Error().Err(err).Msg("the process was running, but was not able to simulate Ctrl+C")
			return
		}

		maxAttempts := 10
		for range maxAttempts {
			// Wait until the process has exited
			iresp, err := cli.ExecInspect(context.Background(), execID, client.ExecInspectOptions{})
			if err != nil {
				log.Fatal(err)
			}
			if !iresp.Running {
				logger.Info().Msg("the process has exited")
				return
			}
			waitDuration := time.Millisecond * 250
			logger.Info().Str("wait_duration", waitDuration.String()).Str("cmd", strings.Join(cmd, " ")).Msg("waiting on exit")
			time.Sleep(waitDuration)
		}

		logger.Error().Int("max_attempts", maxAttempts).Str("cmd", strings.Join(cmd, " ")).Msg("gave up after waiting too many times on process")
	}()

	// read the output
	outputDone := make(chan error)
	parserDone := make(chan error)

	pipeReader, pipeWriter := io.Pipe()
	outScanner := bufio.NewScanner(pipeReader)

	go func() {
		// Since we write both streams to the same pipeWriter the
		// reason we use docker stdcopy instead if io.Copy is to clean
		// up the byte prefix of the messages added by docker StdWriter
		_, err = stdcopy.StdCopy(pipeWriter, pipeWriter, aresp.Reader)

		// Signal to scanner that we are done
		err = pipeWriter.Close()
		if err != nil {
			logger.Error().Err(err).Msg("unable to close pipe")
		}
		outputDone <- err
	}()

	go func() {
		err = runParser(outScanner, logger, debug, msgChan, sender, containerName)
		parserDone <- err
	}()

	select {
	case err := <-outputDone:
		if err != nil {
			return err
		}
		break

	case <-ctx.Done():
		return ctx.Err()
	}

	err = <-parserDone
	if err != nil {
		return fmt.Errorf("parserDone returned error: %w", err)
	}

	return nil
}

func runParser(scanner *bufio.Scanner, logger zerolog.Logger, debug bool, msgChan chan []byte, sender string, containerName string) error {
	var err error

	// *   << Request  >> 2392453
	// -   Begin          req 2392452 rxreq
	// -   ReqStart       192.168.15.85 62353 purger
	// -   ReqURL         /
	// -   ReqHeader      host: cdn-test-backend.cdn.sunet.se
	// -   ReqHeader      user-agent: curl/8.7.1
	// -   ReqHeader      accept: */*
	// -   ReqHeader      X-Forwarded-For: 192.168.15.85
	// -   ReqHeader      Via: 1.1 381a22f21525 (Vinyl-Cache/9.1)
	// -   VCL_call       RECV
	// -   ReqHeader      X-Forwarded-Proto: https
	// -   VCL_call       HASH
	// -   VCL_call       PURGE
	// -   VCL_call       SYNTH
	// -   End

	fieldMap := map[string]int{
		"tag":   3,
		"value": 9,
	}

	// Variables that will need to be reset any time we read a new
	// vinyllog header, see RESET comment below.
	var reqURL string
	var header http.Header
	var clientIP netip.Addr
	var fromPurger bool     // request was sent by a sunet-cdn-purger
	var purgerOrigin string // sender name from the marker header
	var recvStarted bool    // vcl_recv has started, stop collecting
	var clientTLS bool
	var forwardedProtoSeen bool
	seenHeader := false

	for scanner.Scan() {
		text := scanner.Text()
		if debug {
			fmt.Printf("vinyllog line: %s\n", text)
		}
		if strings.HasPrefix(text, "*") {
			if seenHeader {
				logger.Fatal().Msg("found new vinyllog header before seeing 'End' of previous entry, this is odd")
			}

			if debug {
				logger.Debug().Msg("found vinyllog header, resetting variables")
			}
			seenHeader = true

			// RESET: Reset variables for new request we
			// are about to parse
			reqURL = ""
			header = http.Header{}
			clientIP = netip.Addr{}
			fromPurger = false
			purgerOrigin = ""
			recvStarted = false
			clientTLS = false
			forwardedProtoSeen = false
		} else if strings.HasPrefix(text, "-") {
			if !seenHeader {
				logger.Fatal().Msg("got log message without seeing header first, this is odd")
			} else {
				// Maybe regex is more suitable to parse this
				// but keep to easy space-splitting for now.
				// There is also strings.Fields() but I am
				// worried we will lose information regarding
				// the exakt space contents inside header
				// values if we use that.
				// example result: []string{"-", "", "", "ReqHeader", "", "", "", "", "", "user-agent: curl/8.4.0"}
				fields := strings.SplitN(text, " ", 10)

				// Because vinyllog adds spaces to make
				// pretty columns it is possible there is
				// leftover leading space in the value. Clean
				// that up.
				trimmedValue := strings.TrimLeftFunc(fields[fieldMap["value"]], func(r rune) bool {
					// We could use unicode.IsSpace() here,
					// but be strict about the specific
					// whitespace characters we remove for
					// now.
					return r == ' '
				})
				if debug {
					logger.Debug().Str("vinyllog_tag", fields[fieldMap["tag"]]).Str("vinyllog_value", trimmedValue).Msg("vinyllog fields")
				}

				tag := fields[fieldMap["tag"]]
				switch tag {
				case "ReqStart":
					if debug {
						logger.Debug().Str("trimmed_value", trimmedValue).Msg("got ReqStart")
					}
					reqStartFields := strings.Fields(trimmedValue)

					clientIP, err = netip.ParseAddr(reqStartFields[0])
					if err != nil {
						logger.Error().Err(err).Msg("unable to parse ReqStart IP address")
					}
					// Only the purger can connect to this listener, and unlike a
					// header the listener name can't be set by a client.
					if len(reqStartFields) >= 3 && reqStartFields[2] == purgerListenerName {
						fromPurger = true
					}
				case "VCL_call":
					// Everything logged before the first vcl_recv is the
					// request as received; later changes come from the
					// tenant VCL and will be reapplied by the receiving node.
					if trimmedValue == "RECV" {
						recvStarted = true
					}
				case "ReqURL":
					if debug {
						logger.Debug().Str("url", trimmedValue).Msg("got URL: %s\n")
					}
					if !recvStarted {
						reqURL = trimmedValue
					}
				case "ReqHeader", "ReqUnset":
					if recvStarted {
						// The manager block in vcl_recv sets
						// X-Forwarded-Proto from proxy.is_ssl(); the first
						// value set after RECV is the TLS state.
						if tag == "ReqHeader" && !forwardedProtoSeen {
							if key, value, ok := strings.Cut(trimmedValue, ":"); ok && strings.EqualFold(key, "X-Forwarded-Proto") {
								clientTLS = strings.TrimLeft(value, " ") == "https"
								forwardedProtoSeen = true
							}
						}
						continue
					}
					key, value, ok := strings.Cut(trimmedValue, ":")
					if !ok {
						logger.Error().Str("value", trimmedValue).Msg("unable to split header string")
						continue
					}
					value = strings.TrimLeft(value, " ")

					if strings.EqualFold(key, purgerOriginHeader) {
						// Informational only, detection uses the listener name.
						purgerOrigin = value
						continue
					}
					if unpropagatedHeaders[http.CanonicalHeaderKey(key)] {
						continue
					}

					if tag == "ReqHeader" {
						header.Add(key, value)
					} else {
						removeHeaderValue(header, key, value)
					}
				case "End":
					// The entry is complete, finish
					// filling in struct and send it to
					// MQTT.
					seenHeader = false

					if fromPurger {
						logger.Info().Str("origin_sender", purgerOrigin).Msg("not sending MQTT message for purge request sent by sunet-cdn-purger")
						continue
					}

					if clientIP.IsLoopback() {
						// In order to not create loops
						// we ignore PURGE requests
						// that come from our own
						// machine as this could be
						// a purge request sent by this
						// process in response to a
						// MQTT message from someone else.
						//
						// This could also be a result
						// of running a local curl or
						// similar, and we probably
						// should not spread those
						// either.
						logger.Info().Str("client_ip", clientIP.String()).Msg("not sending MQTT message for purge request from localhost")
						continue
					}

					if !forwardedProtoSeen {
						logger.Warn().Msg("no X-Forwarded-Proto set in vcl_recv, propagating purge as plain HTTP")
					}

					for _, k := range coreAppendedHeaders {
						stripLastListEntry(header, k)
					}

					pm := purgeMessage{
						Sender:        sender,
						Header:        header,
						ContainerName: containerName,
						ClientIP:      clientIP,
						ClientTLS:     clientTLS,
					}

					// Varnish sees all requests as http:// since TLS is terminated by haproxy
					urlString := "http://"
					if hosts := header.Values("host"); hosts != nil {
						if len(hosts) != 1 {
							logger.Error().Int("num_hosts", len(hosts)).Msg("unexpected number of host header fields found")
						} else {
							urlString += hosts[0]
						}
					}

					urlString += reqURL

					u, err := url.Parse(urlString)
					if err != nil {
						logger.Error().Err(err).Msg("unable to parse URL")
					} else {
						pm.URL = u
					}

					var b []byte

					if debug {
						b, err = json.MarshalIndent(pm, "", "  ")
					} else {
						b, err = json.Marshal(pm)
					}
					if err != nil {
						logger.Error().Err(err).Msg("unable to marshal purgeMessage")
					} else {
						if debug {
							fmt.Println("about to send the following data:")
							fmt.Println(string(b))
						}
						msgChan <- b
						// purgesSent.Inc()
					}

				}
			}
		} else if text == "" {
			// Do nothing, each log entry is trailed by an empty line
		} else if text == "^C" {
			// Do nothing, if we simulate the sending of Ctrl+C this character will appear
		} else {
			logger.Error().Str("vinyllog_line", text).Msg("found unexpected vinyllog line")
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Fatal().Err(err).Msg("scanner failed")
	}

	logger.Info().Msg("runParser: exiting")
	return nil
}

// https://www.haproxy.org/download/3.1/doc/proxy-protocol.txt
var proxyV2Signature = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}

// proxyV2Header builds a PROXY protocol v2 header for src. When isTLS is
// set it includes a PP2_TYPE_SSL TLV so proxy.is_ssl() is true in VCL.
func proxyV2Header(src netip.Addr, isTLS bool) ([]byte, error) {
	src = src.Unmap()

	var family byte
	var body []byte
	if src.Is4() {
		family = 0x11 // AF_INET, STREAM
		s, d := src.As4(), netip.MustParseAddr("127.0.0.1").As4()
		body = append(body, s[:]...)
		body = append(body, d[:]...)
	} else {
		family = 0x21 // AF_INET6, STREAM
		s, d := src.As16(), netip.IPv6Loopback().As16()
		body = append(body, s[:]...)
		body = append(body, d[:]...)
	}

	dstPort := uint16(80)
	if isTLS {
		dstPort = 443
	}
	body = binary.BigEndian.AppendUint16(body, 40000) // source port
	body = binary.BigEndian.AppendUint16(body, dstPort)

	if isTLS {
		// PP2_TYPE_SSL (0x20), length 5: client flags PP2_CLIENT_SSL,
		// then a non-zero verify result (no verified client certificate).
		body = append(body, 0x20, 0x00, 0x05, 0x01, 0x00, 0x00, 0x00, 0x01)
	}

	h := append([]byte{}, proxyV2Signature...)
	h = append(h, 0x21, family) // version 2, PROXY command

	// G115 (CWE-190): integer overflow conversion int -> uint16 (Confidence: MEDIUM, Severity: HIGH)
	bodyLen := len(body)
	if bodyLen > math.MaxUint16 {
		return nil, fmt.Errorf("body length is too large for uint16")
	}
	h = binary.BigEndian.AppendUint16(h, uint16(bodyLen))
	return append(h, body...), nil
}

// newPurgeClient returns a client whose connections start with a PROXY
// header for src. Vinyl reads the PROXY header once per connection and
// applies it to every request sent on it, so a client must only be used
// for a single purge and never shared, otherwise a later purge could be
// treated as coming from an earlier purge's client.
func newPurgeClient(logger zerolog.Logger, socketPath string, src netip.Addr, isTLS bool) (*http.Client, error) {
	if !src.IsValid() {
		return nil, fmt.Errorf("purge message has no valid client IP")
	}
	proxyHeader, err := proxyV2Header(src, isTLS)
	if err != nil {
		return nil, err
	}

	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			// Don't add "Accept-Encoding: gzip", the request should only
			// carry the headers the original had.
			DisableCompression: true,
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				conn, err := dialUnixLongPath(ctx, logger, socketPath)
				if err != nil {
					return nil, err
				}
				if _, err := conn.Write(proxyHeader); err != nil {
					if cErr := conn.Close(); cErr != nil {
						logger.Err(cErr).Msg("unable to close unix dial context")
					}
					return nil, err
				}
				return conn, nil
			},
		},
	}, nil
}

func vinyllogReader(ctx context.Context, containerName string, containerID string, wg *sync.WaitGroup, msgChan chan []byte, logger zerolog.Logger, sender string, debug bool, dockerClient *client.Client, mc *monitoredContainers) {
	defer wg.Done()

	defer func() {
		logger.Info().Str("name", containerName).Msg("cleaning up no longer monitored container from monitoredContainers map")
		mc.del(containerName)
	}()

	vinyllogCmd := []string{"vinyllog", "-n", "/var/lib/vinyl-cache/vinyld", "-q", `ReqMethod eq "PURGE" and RespStatus == 200 and ReqURL`, "-i", "Begin,ReqHeader,ReqUnset,ReqURL,ReqStart,VCL_call,End"}

	err := dockerExec(ctx, dockerClient, containerID, vinyllogCmd, logger, debug, msgChan, sender, containerName)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			logger.Fatal().Err(err).Msg("dockerExec failed")
		}
	}

	logger.Info().Str("name", containerName).Msg("vinyllogReader: exiting")
}

func setupMQTT(ctx context.Context, debug bool, logger *zerolog.Logger, logDir string, hostname string, serverURL *url.URL, tlsConfig *tls.Config, subTopic string, handleLocalMessages bool) (*autopaho.ConnectionManager, chan *paho.Publish, error) {
	q, err := file.New(logDir, "queue", ".msg")
	if err != nil {
		return nil, nil, fmt.Errorf("setupMQTTPub(): unable to create file queue: %w", err)
	}

	subChan := make(chan *paho.Publish)

	errorLogger := logger.With().Str("paho_logger", "errors").Logger()
	pahoErrorLogger := logger.With().Str("paho_logger", "paho_errors").Logger()

	cliCfg := autopaho.ClientConfig{
		Queue:                         q,
		ServerUrls:                    []*url.URL{serverURL},
		TlsCfg:                        tlsConfig,
		KeepAlive:                     20,        // Keepalive message should be sent every 20 seconds
		CleanStartOnInitialConnection: false,     // Keep old messages in the broker in case we are missing
		SessionExpiryInterval:         86400 * 7, // If connection drops we want to keep messages in the broker for 7 days
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			logger.Info().Msg("pubsub: mqtt connection up")
			if _, err := cm.Subscribe(ctx, &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{
						Topic:   subTopic,
						QoS:     1,
						NoLocal: !handleLocalMessages, // No reason to recieve a message we sent ourselves other than for testing purposes
					},
				},
			}); err != nil {
				logger.Error().Err(err).Msg("subscribe: failed to subscribe. Probably due to connection drop so will retry")
				return // likely connection has dropped
			}
			logger.Info().Msg("subscribe: mqtt subscription made")
		},
		OnConnectError: func(err error) { logger.Error().Err(err).Msg("pubsub: error whilst attempting connection") },
		Errors:         &errorLogger,
		PahoErrors:     &pahoErrorLogger,
		// eclipse/paho.golang/paho provides base mqtt functionality, the below config will be passed in for each connection
		ClientID: "sunet-cdn-purger-" + hostname,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				subChan <- pr.Packet
				return true, nil
			},
		},
		OnClientError: func(err error) { logger.Error().Err(err).Msg("pubsub: client error") },
		OnServerDisconnect: func(d *paho.Disconnect) {
			if d.Properties != nil {
				logger.Info().Str("reason_string", d.Properties.ReasonString).Msg("pubsub: server requested disconnect")
			} else {
				logger.Info().Uint8("reason_code", uint8(d.ReasonCode)).Msg("pubsub server requested disconnect")
			}
		},
	}

	// Do not keep session around for long if we are doing testing stuff
	if handleLocalMessages {
		cliCfg.SessionExpiryInterval = 60
	}

	if debug {
		debugLogger := logger.With().Str("paho_logger", "debug").Logger()
		pahoDebugLogger := logger.With().Str("paho_logger", "paho_debug").Logger()

		cliCfg.Debug = &debugLogger
		cliCfg.PahoDebug = &pahoDebugLogger
	}

	c, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("setupMQTT: unable to create connection: %w", err)
	}

	return c, subChan, nil
}

func newLogChain(logger zerolog.Logger) alice.Chain {
	c := alice.New()

	c = c.Append(hlog.NewHandler(logger))

	// Install some provided extra handler to set some request's context fields.
	// Thanks to that handler, all our logs will come with some prepopulated fields.
	c = c.Append(hlog.AccessHandler(func(r *http.Request, status, size int, duration time.Duration) {
		hlog.FromRequest(r).Info().
			Str("method", r.Method).
			Stringer("url", r.URL).
			Int("status", status).
			Int("size", size).
			Dur("duration", duration).
			Msg("")
	}))
	c = c.Append(hlog.RemoteAddrHandler("ip"))
	c = c.Append(hlog.UserAgentHandler("user_agent"))
	c = c.Append(hlog.RefererHandler("referer"))
	c = c.Append(hlog.RequestIDHandler("req_id", "Request-Id"))

	return c
}

func certPoolFromFile(fileName string) (*x509.CertPool, error) {
	fileName = filepath.Clean(fileName)
	cert, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("certPoolFromFile: unable to read file: %w", err)
	}
	certPool := x509.NewCertPool()
	ok := certPool.AppendCertsFromPEM([]byte(cert))
	if !ok {
		return nil, fmt.Errorf("certPoolFromFile: failed to append certs from pem: %w", err)
	}

	return certPool, nil
}

type certStore struct {
	mutex      sync.RWMutex
	clientCert *tls.Certificate
}

func (cs *certStore) setClientCertificate(certFile string, keyFile string) error {
	// Setup client cert/key for mTLS authentication
	clientCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("unable to load x509 MQTT client cert: %w", err)
	}

	cs.mutex.Lock()
	cs.clientCert = &clientCert
	cs.mutex.Unlock()

	return nil
}

func (cs *certStore) getClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	return cs.clientCert, nil
}

func newCertStore() *certStore {
	return &certStore{}
}

func main() {
	debug := flag.Bool("debug", false, "enable debug logging")
	mqttClientCertFile := flag.String("mqtt-client-cert-file", "", "MQTT client cert file")
	mqttClientKeyFile := flag.String("mqtt-client-key-file", "", "MQTT client key file")
	mqttCAFile := flag.String("mqtt-ca-file", "", "MQTT trusted CA file, leave empty for OS default")
	mqttQueueDir := flag.String("mqtt-queue-dir", "/var/cache/sunet-cdn-purger/mqtt", "MQTT message queue directory")
	mqttPubTopic := flag.String("mqtt-pub-topic", "test/topic", "the topic we publish PURGE messages to")
	mqttSubTopic := flag.String("mqtt-sub-topic", "test/topic", "the topic we subscribe to PURGE messages on")
	mqttServerString := flag.String("mqtt-server", "tls://localhost:8883", "the MQTT server we connect to")
	httpServerAddr := flag.String("http-server-addr", "127.0.0.1:2112", "Address to bind HTTP server to")
	handleLocalMessages := flag.Bool("danger-handle-local-messages", false, "Handle messages sent by ourselves, should only be enabled for testing")
	containerPrefix := flag.String("container-prefix", "sunet-cdn-agent_cache_", "Container name prefix for things we attach vinyllog to")
	containerQualifier := flag.String("container-qualifier", "-vinyl-", "Additional container name contents for things we attach vinyllog to")
	socketPath := flag.String("socket-path", "/purger/unix-sockets/vinyl", "Unix socket of the local vinyl PROXY listener used for sending purges")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := zerolog.New(os.Stdout).With().
		Timestamp().
		Str("service", "sunet-cdn-purger").
		Logger()

	sender, err := os.Hostname()
	if err != nil {
		logger.Fatal().Err(err).Msg("unable to get hostname")
	}

	logger = logger.With().Str("sender", sender).Logger()

	newLogChain := newLogChain(logger)

	serverURL, err := url.Parse(*mqttServerString)
	if err != nil {
		logger.Fatal().Err(err).Msg("unable to parse server string")
	}

	// Leaving these nil will use the OS default CA certs
	var mqttCACertPool *x509.CertPool

	// Setup client cert/key for mTLS authentication
	cs := newCertStore()
	err = cs.setClientCertificate(*mqttClientCertFile, *mqttClientKeyFile)
	if err != nil {
		logger.Fatal().Err(err).Msg("unable to load x509 MQTT client cert")
	}

	// Create new watcher.
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Fatal().Err(err).Msg("unable to create fsnotify watcher")
	}
	defer func() {
		err := watcher.Close()
		if err != nil {
			logger.Err(err).Msg("unable to close watcher")
		}
	}()

	// Start listening for events.
	go func() {
		// Event dedup based on https://github.com/fsnotify/fsnotify/blob/main/cmd/fsnotify/dedup.go
		var mutex sync.Mutex
		timers := make(map[string]*time.Timer)

		callback := func(e fsnotify.Event) {
			if e.Name == *mqttClientCertFile {
				logger.Info().Msg("reloading MQTT client certificate")
				err := cs.setClientCertificate(*mqttClientCertFile, *mqttClientKeyFile)
				if err != nil {
					logger.Err(err).Msg("reloading MQTT client certificate failed")
				}
			}

			mutex.Lock()
			delete(timers, e.Name)
			mutex.Unlock()
		}

		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}

				if !event.Has(fsnotify.Write) && !event.Has(fsnotify.Create) {
					continue
				}

				// Get timer.
				mutex.Lock()
				t, ok := timers[event.Name]
				mutex.Unlock()

				// No timer exists, create it and stop it from running.
				if !ok {
					t = time.AfterFunc(math.MaxInt64, func() { callback(event) })
					t.Stop()

					mutex.Lock()
					timers[event.Name] = t
					mutex.Unlock()
				}

				// Reset the timer for this path so it will
				// run callback() in 100ms. If additional
				// events appear for the same file we will keep
				// resetting the timer.
				t.Reset(100 * time.Millisecond)

			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				logger.Err(err).Msg("watcher error")
			}
		}
	}()

	// Add a path.
	err = watcher.Add(filepath.Dir(*mqttClientCertFile))
	if err != nil {
		logger.Fatal().Err(err).Str("dir", filepath.Dir(*mqttClientCertFile)).Msg("unable to add fsnotify dir")
	}

	mqttCACertPool, err = certPoolFromFile(*mqttCAFile)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create CA cert pool")
	}

	tlsCfg := &tls.Config{
		RootCAs:              mqttCACertPool,
		GetClientCertificate: cs.getClientCertificate,
		MinVersion:           tls.VersionTLS13,
	}

	// Make sure the queue dir exists
	err = os.MkdirAll(*mqttQueueDir, 0o750)
	if err != nil {
		logger.Fatal().Err(err).Msg("unable to create MQTT queue directory")
	}

	hostname, err := os.Hostname()
	if err != nil {
		logger.Fatal().Err(err).Msg("unable to lookup hostname")
	}

	mqttCM, subMsgChan, err := setupMQTT(ctx, *debug, &logger, *mqttQueueDir, hostname, serverURL, tlsCfg, *mqttSubTopic, *handleLocalMessages)
	if err != nil {
		logger.Fatal().Err(err).Msg("unable to setup MQTT publisher")
	}

	pubPayloadChan := make(chan []byte)
	mh := newLogChain.Then(promhttp.Handler())

	srv := &http.Server{
		Addr:           *httpServerAddr,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	idleConnsClosed := make(chan struct{})
	go func() {
		<-ctx.Done()

		logger.Info().Msg("received signal, shutting down metrics HTTP server")

		// We received an interrupt signal, shut down.
		if err := srv.Shutdown(context.Background()); err != nil {
			// Error from closing listeners, or context timeout:
			logger.Error().Err(err).Msg("HTTP server Shutdown() error")
		}
		close(idleConnsClosed)
	}()

	dockerClient, err := client.New(client.FromEnv)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := dockerClient.Close()
		if err != nil {
			logger.Err(err).Msg("unable to close dockerClient")
		}
	}()

	var wg sync.WaitGroup

	wg.Add(1)
	go messagePublisher(ctx, &wg, pubPayloadChan, logger, mqttCM, *mqttPubTopic)
	wg.Add(1)
	go messageSubscriber(ctx, &wg, subMsgChan, logger, sender, *debug, *handleLocalMessages, *socketPath, dockerClient)
	wg.Add(1)
	go containerMonitor(ctx, &wg, pubPayloadChan, logger, sender, *debug, dockerClient, *containerPrefix, *containerQualifier)

	http.Handle("/metrics", mh)

	err = srv.ListenAndServe()
	if err != http.ErrServerClosed {
		logger.Fatal().Err(err).Msg("HTTP server failed unexpectedly")
	}

	// Wait for connections to be gracefully closed.
	logger.Info().Msg("waiting for HTTP connections to complete")
	<-idleConnsClosed

	// The MQTT connection is automatically disconnected when a
	// signal is recevied by our signal.NotifyContext().
	// Wait here in case things have not finished shutting down yet.
	logger.Info().Msg("waiting for MQTT connection handler to shutdown")
	<-mqttCM.Done()

	// The HTTP server and MQTT connection is down at this point so our
	// message handlers can stop working now:
	close(pubPayloadChan)
	close(subMsgChan)

	// Wait for the message handlers to exit.
	logger.Info().Msg("waiting for message handlers to exit")
	wg.Wait()
}
