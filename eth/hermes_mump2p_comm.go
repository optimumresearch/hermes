package eth

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"

	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	eth "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	pb "github.com/probe-lab/hermes/eth/pb/telemetry"
	"google.golang.org/protobuf/proto"

	log "github.com/sirupsen/logrus"
	//protobuf "github.com/OffchainLabs/prysm/v7/beacon-chain/pb"
	protobuf "github.com/probe-lab/hermes/eth/pb/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"github.com/golang/snappy"
	ethtypes "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
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

		// 1. Connect to the client
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
			log.Warn("======================failed to connect to local mump2p: %v", err)
			streamError = true
			time.Sleep(2 * time.Second) // small delay before moving on
			continue
		}
		defer func() {
			if err := conn.Close(); err != nil {
				log.WithError(err).Error("Failed to close the conn")
			}
		}()

		client := protobuf.NewCommandStreamClient(conn)

		// 2.  Create stream with context
		streamCtx, streamCancel := context.WithCancel(ctx)
		defer streamCancel()

		stream, err := client.ListenCommands(streamCtx)
		if err != nil {
			streamError = true
			time.Sleep(2 * time.Second) // small delay before moving on
			log.Warnf("======================================failed to create stream to mump2p: %w", err)
			continue
		}

		log.Infof("Connected to mump2p node %s to send\n", ip)

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
					log.Warn("=======================================SEND message in sendMessages")
					if err := stream.Send(pubReq); err != nil {
						log.Warn("======================================= Failed SEND message in sendMessages")
						return
					}
				}
			}

			// Close send side when done
			if err := stream.CloseSend(); err != nil {
				log.WithError(err).Warn("=============================Failed to close send stream")
			}
		}()

		// ----- WAIT AND MONITOR -----
		wg.Wait()
		if streamError {
			time.Sleep(2 * time.Second) // small delay before moving on
		}
	}
}

func writeToFile(ctx context.Context, dataCh <-chan string, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Errorf(fmt.Sprintf("Cannot open file %s", filename))
		}
	}()

	writer := bufio.NewWriter(file)
	defer func() {
		if err := writer.Flush(); err != nil {
			log.Error(fmt.Sprintf("Cannot open the writer %s", filename))
		}
	}()

	// Process until channel is closed
	for data := range dataCh {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if _, err = writer.WriteString(data + "\n"); err != nil {
			log.Errorf("Failed to write the data to %s", filename)
		}

		if err := writer.Flush(); err != nil {
			log.Errorf("Failed to flush the data to %s", filename)
		}
	}
	fmt.Println("All data flushed to disk")
}

// gets the basic block information from the proto.Message that comes from the Gossip
// Parameters:
//   - msg: the gossip message for the beacon block
//
// Returns:
//   - string with basic information on the block
//   - err
func getBeaconBlockInfo(msg proto.Message) (string, error) {
	signed, err := blocks.NewSignedBeaconBlock(msg)
	if err != nil {
		return "", err
	}
	if err := blocks.BeaconBlockIsNil(signed); err != nil {
		return "", err
	}

	block := signed.Block()
	body := block.Body()

	// Try to get execution payload info
	blockHash := ""
	blockNumber := uint64(0)
	blockSize := int(0)

	if executionPayload, err := body.Execution(); err == nil {
		blockSize = len(executionPayload.ExtraData()) + 32*5 // Basic size estimation
		blockHash = fmt.Sprintf("%#x", executionPayload.BlockHash())
		blockNumber = executionPayload.BlockNumber()

		txns, _ := executionPayload.Transactions()
		for _, tx := range txns {
			blockSize += len(tx)
		}
	}

	timeUnix := time.Now().UnixMilli()
	blockInfoString := fmt.Sprintf("%d\t%d\t%s\t%d\t%d", blockNumber, block.Slot(), blockHash, timeUnix, blockSize)

	return blockInfoString, nil
}

func getBeaconBlockInfoLogEntry(msg proto.Message) (*pb.LogEntry, error) {
	signed, err := blocks.NewSignedBeaconBlock(msg)
	if err != nil {
		return nil, err
	}
	if err := blocks.BeaconBlockIsNil(signed); err != nil {
		return nil, err
	}

	block := signed.Block()
	body := block.Body()

	// Try to get execution payload info
	blockHash := ""
	blockNumber := uint64(0)
	blockSize := int(0)

	if executionPayload, err := body.Execution(); err == nil {
		blockSize = len(executionPayload.ExtraData()) + 32*5 // Basic size estimation
		blockHash = fmt.Sprintf("%#x", executionPayload.BlockHash())
		blockNumber = executionPayload.BlockNumber()

		txns, _ := executionPayload.Transactions()
		for _, tx := range txns {
			blockSize += len(tx)
		}
	}

	timeUnix := time.Now().UnixMilli()
	logEntry := &pb.LogEntry{
		Blocknumber: int64(blockNumber),
		Slot:        int64(block.Slot()),
		Blockhash:   blockHash,
		Timestamp:   timeUnix,
		Blocksize:   int64(blockSize),
		ClientName:  "host",
		Type:  "MUMP2P",
	}
	
	return logEntry, nil
}


// gets the basic block information from eth.SignedBeaconBlockFulu, the that comes from the Gossip
// Parameters:
//   - msg: the gossip message for the beacon block
//
// Returns:
//   - string with basic information on the block
//   - err
func getBeaconBlockInfo1(fuluBlock *eth.SignedBeaconBlockFulu) (string, error) {
	// 2. Access the Slot number, which is nested within the block's Message.
	slotNumber := fuluBlock.GetBlock().Slot
	blockNumber := fuluBlock.GetBlock().Body.ExecutionPayload.BlockNumber
	blockHash := fuluBlock.GetBlock().Body.ExecutionPayload.BlockHash
	blockSize := fuluBlock.GetBlock().Body.SizeSSZ()
	timeUnix := time.Now().UnixMilli()

	blockInfoString := fmt.Sprintf("%d\t%d\t%#x\t%d\t%d", blockNumber, slotNumber, blockHash, timeUnix, blockSize)

	return blockInfoString, nil

}

func getBeaconBlockInfo2(fuluBlock *eth.SignedBeaconBlockFulu, hostID string) (*pb.LogEntry, error) {
	// 2. Access the Slot number, which is nested within the block's Message.
	slotNumber := fuluBlock.GetBlock().Slot
	blockNumber := fuluBlock.GetBlock().Body.ExecutionPayload.BlockNumber
	blockHash := fuluBlock.GetBlock().Body.ExecutionPayload.BlockHash
	blockSize := fuluBlock.GetBlock().Body.SizeSSZ()
	timeUnix := time.Now().UnixMilli()

	logEntry := &pb.LogEntry{
		Blocknumber: int64(blockNumber),
		Slot:        int64(slotNumber),
		Blockhash:   fmt.Sprintf("%#x", blockHash),
		Timestamp:   timeUnix,
		Blocksize:   int64(blockSize),
		ClientName:  hostID,
		Type:  "MAINNET",
	}
	return logEntry, nil
}

func receiveMessages(ctx context.Context, ip string, topic string) error {
	for {
		// 1. Check context before trying to connect
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := runStream(ctx, ip, topic); err != nil {
			log.Warnf("Stream failed: %v. Retrying in 2s...", err)
			time.Sleep(2 * time.Second)
		}
	}
}

// Separate function handles the lifecycle of ONE connection
func runStream(ctx context.Context, ip string, topic string) error {
	conn, err := grpc.NewClient(ip,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt),
			grpc.MaxCallSendMsgSize(math.MaxInt),
		),
	)
	if err != nil {
		return fmt.Errorf("dial error: %w", err)
	}
	defer conn.Close() // Now triggers correctly when runStream returns

	client := protobuf.NewCommandStreamClient(conn)
	stream, err := client.ListenCommands(ctx)
	if err != nil {
		return fmt.Errorf("stream creation failed: %w", err)
	}

	// Subscribe logic...
	subReq := &protobuf.Request{Command: int32(CommandSubscribeToTopic), Topic: topic}
	if err := stream.Send(subReq); err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	// Process messages until an error occurs
	for {
		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("receive error: %w", err)
		}

		// Handle your message here directly
		// (Using a channel is only necessary if handling is slower than receiving)
		// Pass the response and your destination channel
		handleMessage(resp, dataMump2pToHermesCh)
	}
}

func handleMessage(resp *protobuf.Response, dataOut chan<- string) {
	// 1. Verify we have the correct response type
	if resp.GetCommand() != protobuf.ResponseType_Message {
		return
	}

	// 2. Unmarshal the outer P2P wrapper
	var p2pMessage P2PMessage
	timeUnix := time.Now().UnixMilli()
	if err := json.Unmarshal(resp.GetData(), &p2pMessage); err != nil {
	        defaultStringToWrite := fmt.Sprintf("%d\t%d\t%s\t%d\t%d", 1111111, 111111, "0xabcd", timeUnix, 100)
	        dataOut <- defaultStringToWrite
		log.WithError(err).Error("Failed to unmarshal outer P2P message")
		return
	}

	// 3. Convert the raw P2P data into a Beacon Block
	signedBlock, err := unmarshalMumP2PMessageToBlock(p2pMessage.Message)
	if err != nil {
	        defaultStringToWrite := fmt.Sprintf("%d\t%d\t%s\t%d\t%d", 2222222, 2222222, "0xabcd", timeUnix, 100)
	        dataOut <- defaultStringToWrite

		log.WithError(err).Error("Cannot unmarshal data received from Prysm")
		return
	}

	// 4. Extract specific info (e.g., Slot, Root, or State)
	strToWrite, err := getBeaconBlockInfo(signedBlock)
	if err != nil {
	        defaultStringToWrite := fmt.Sprintf("%d\t%d\t%s\t%d\t%d", 333333, 3333333, "0xabcd", timeUnix, 100)
	        dataOut <- defaultStringToWrite
		log.WithError(err).Error("Cannot get beacon block info")
		return
	}

	// 5. Send to the processing channel
	// Note: If the channel is full, this will block.
	// Use a 'select' with a default if you prefer dropping messages over blocking.
	dataOut <- strToWrite

        // 6. The LogEntry to send to the server
        logEntry, err := getBeaconBlockInfoLogEntry(signedBlock)
	if err != nil {
		log.Errorf("Cannot get basic information 2 from block as logEntry %v", err)
	}
        dataHermesToServer <- logEntry
}

func unmarshalMumP2PMessageToBlock(data []byte) (*ethtypes.SignedBeaconBlockFulu, error) {
	// 1. Decompress (Snappy Decode)
	// snappy.Decode requires a destination buffer or nil to allocate a new one
	rawBytes, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, fmt.Errorf("failed to snappy decode: %w", err)
	}

	// 2. Initialize an empty Fulu block struct
	// Note: Use the same version (Fulu) as the sender
	fuluBlock := new(ethtypes.SignedBeaconBlockFulu)


	// 3. Unmarshal (SSZ Decode)
	// This populates the fuluBlock pointer with the data from rawBytes
	if err := fuluBlock.UnmarshalSSZ(rawBytes); err != nil {
		return nil, fmt.Errorf("failed to unmarshal SSZ: %w", err)
	}

	return fuluBlock, nil
}


/*
	hash := sha256.Sum256(p2pMessage.Message)
	hexHashString := hex.EncodeToString(hash[:])

	fmt.Printf("RECV: %s; publisher: %s; size: %v\n", hexHashString, "Gateway", len(p2pMessage.Message))
*/
