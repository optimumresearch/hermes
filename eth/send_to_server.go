package eth

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
	"time"
	//pb "pb/telemetry"
	"io"
	"sync"

	//protobuf "github.com/OffchainLabs/prysm/v7/beacon-chain/pb"
	protobuf "github.com/probe-lab/hermes/eth/pb/protobuf"
	pb "github.com/probe-lab/hermes/eth/pb/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// P2PMessage represents a message structure used in P2P communication
type P2PMessage struct {
	MessageID    string // Unique identifier for the message
	Topic        string // Topic name where the message was published
	Message      []byte // Actual message data
	SourceNodeID string // ID of the node that sent the message (we don't need it in future, it is just for debug purposes)
}

// Command possible operation that sidecar may perform with p2p node
type Command int32

const (
	CommandUnknown Command = iota
	CommandPublishData
	CommandSubscribeToTopic
	CommandUnSubscribeToTopic
)

func sendMessages(ctx context.Context, ip string, topic string) error {

	for {
		streamError := false
		// Create connection with timeout
		conn, err := grpc.NewClient(ip,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(100*1024*1024), // 100MB, not MaxInt
				grpc.MaxCallSendMsgSize(100*1024*1024),
			),
			grpc.WithBlock(), // Wait for connection
			grpc.WithTimeout(10*time.Second),
		)
		if err != nil {
			return fmt.Errorf("failed to connect: %w", err)
		}
		defer func() {
			if err := conn.Close(); err != nil {
				log.WithError(err).Error("Failed to close the conn")
			}
		}()

		client := protobuf.NewCommandStreamClient(conn)

		// Create stream with context
		streamCtx, streamCancel := context.WithCancel(ctx)
		defer streamCancel()

		stream, err := client.ListenCommands(streamCtx)
		if err != nil {
			return fmt.Errorf("failed to create stream: %w", err)
		}

		// Create buffered error channel
		errChan := make(chan error, 2)
		var wg sync.WaitGroup

		// ----- RECEIVER GOROUTINE (MUST HAVE) -----
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer streamCancel() // Cancel stream if receiver fails

			for {
				select {
				case <-streamCtx.Done():
					return
				default:
					_, err := stream.Recv()
					if err != nil {
						if err == io.EOF {
							log.Info("Stream closed by server")
							return
						}
						return
					}
					// Optional: Handle response
					//log.Warn("Received gRPC response from  in sendMessages")
				}
			}
		}()

		// ----- SENDER GOROUTINE -----
		wg.Add(1)
		go func() {
			defer wg.Done()

			for pubReq := range msgChan {
				select {
				case <-streamCtx.Done():
					return
				default:
					log.Warn("SEND message in sendMessages")
					if err := stream.Send(pubReq); err != nil {
						errChan <- fmt.Errorf("send failed: %w", err)
						return
					}
				}
			}

			// Close send side when done
			if err := stream.CloseSend(); err != nil {
				log.WithError(err).Warn("Failed to close send stream")
			}
		}()

		// ----- WAIT AND MONITOR -----
		go func() {
			wg.Wait()
			close(errChan)
		}()

		select {
		case err := <-errChan:
			return fmt.Errorf("stream error: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		}

		if streamError {
			time.Sleep(2 * time.Second) // small delay before moving on
		}
	}
}

func createData(dataChan chan<- *pb.LogEntry, name string) {
	// 3. Loop every 12 seconds
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	blknum := int64(100000)
	slot := int64(200000)
	blocksize := int64(34000)
	for range ticker.C {
		logEntry := &pb.LogEntry{
			Blocknumber: blknum,
			Slot:        slot,
			Blockhash:   fmt.Sprintf("%x", blknum),
			Timestamp:   time.Now().Unix(),
			Blocksize:   blocksize,
			ClientName:  name,
		}
		//Timestamp: time.Now().Unix(),
		blknum = blknum + 1
		slot = slot + 1
		blocksize = blocksize + 1

		dataChan <- logEntry

		log.Println(name, "Data pushed to central server")
	}
}

func sendData(dataChan <-chan *pb.LogEntry, serverIP string) {
	for {
		streamError := false
		// 1. Connect to the central server
		conn, err := grpc.Dial(serverIP+":50051", grpc.WithInsecure())
		if err != nil {
			log.Warnf("could not open connection: %v", err)
			streamError = true
			time.Sleep(2 * time.Second) // small delay before moving on
			continue
		}

		defer conn.Close()
		client := pb.NewTelemetryServiceClient(conn)

		// 2. Open the stream
		stream, err := client.SendLogs(context.Background())
		if err != nil {
			log.Warnf("could not open stream: %v", err)
			streamError = true
			time.Sleep(2 * time.Second) // small delay before moving on
			continue
		}

		// 3. Loop every 12 seconds
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for logentry := range dataChan {
			if err := stream.Send(logentry); err != nil {
				log.Printf("failed to send: %v", err)
				streamError = true
				break // Handle reconnection logic here
			}
			log.Println(logentry.ClientName, "Data pushed to central server")
		}

		if streamError {
			time.Sleep(2 * time.Second) // small delay before moving on
		}
	}
}

// GetFirstLine reads the first line from a file and returns it trimmed.
// Returns an error if the file doesn't exist or can't be read.
func getServerIP(filename string) (string, error) {
	// Check if file exists
	if _, err := os.Stat(filename); err != nil {
		if os.IsNotExist(err) {
			return "", errors.New("file does not exist")
		}
		return "", err
	}

	// Open the file
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Read the first line
	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		return strings.TrimSpace(scanner.Text()), nil
	}

	// File is empty
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", errors.New("file is empty")
}
