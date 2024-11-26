package firehose

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/CharlesDardaman/blueskyfirehose/diskutil"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/bluesky-social/indigo/util/cliutil"
	logging "github.com/ipfs/go-log"

	"github.com/gorilla/websocket"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("firehose")

var authFile = "bsky.auth"

var Firehose = &cli.Command{
	Name: "firehose",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name: "authed", //if you want to be authed or not.
		},
		&cli.Int64Flag{
			Name:  "mf", //min follower count to print
			Value: 0,
		},
		&cli.BoolFlag{
			Name: "likes", //if you want likes to show or not
		},
		&cli.BoolFlag{
			Name: "save",
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT)
		defer stop()

		if !diskutil.FileExists(authFile) && cctx.Bool("authed") {
			//create session and write it to disk

			if cctx.Args().Len() < 2 {
				return fmt.Errorf("please provide username and password")
			}

			sess, err := createSession(cctx)
			if err != nil {
				return err
			}

			// Saves the bsky.auth file
			err = diskutil.WriteStructToDisk(sess, authFile)
			if err != nil {
				return err
			}
		}

		arg := "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"

		//Set if empty
		if cctx.String("pds-host") == "" {
			cctx.Set("pds-host", "https://bsky.social")
		}

		var err error

		fmt.Println("dialing: ", arg)
		d := websocket.DefaultDialer
		con, _, err := d.Dial(arg, http.Header{})
		if err != nil {
			return fmt.Errorf("dial failure: %w", err)
		}

		fmt.Println("Stream Started", time.Now().Format(time.RFC3339))
		defer func() {
			fmt.Println("Stream Exited", time.Now().Format(time.RFC3339))
		}()

		go func() {
			<-ctx.Done()
			_ = con.Close()
		}()

		// HandleRepoStream is the main beef of this function
		// It will run on each event and switch on the event type and run the callbacks passed to it in
		// events.RepoStreamCallbacks

		rscb := &events.RepoStreamCallbacks{
			RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {

				// Returns a... readrepo?
				rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
				if err != nil {
					fmt.Println(err)
				} else {
					for _, op := range evt.Ops {
						ek := repomgr.EventKind(op.Action)
						switch ek {
						case repomgr.EvtKindCreateRecord, repomgr.EvtKindUpdateRecord:
							//fmt.Println("got record", op.Path, op.Cid, op.Action, evt.Seq, evt.Repo)
							rc, rec, err := rr.GetRecord(ctx, op.Path)
							if err != nil {
								e := fmt.Errorf("getting record %s (%s) within seq %d for %s: %w", op.Path, *op.Cid, evt.Seq, evt.Repo, err)
								log.Error(e)
								return nil
							}

							if lexutil.LexLink(rc) != *op.Cid {
								return fmt.Errorf("mismatch in record and op cid: %s != %s", rc, *op.Cid)
							}

							//fmt.Println("got record", rc, rec)
							banana := lexutil.LexiconTypeDecoder{
								Val: rec,
							}

							var pst = appbsky.FeedPost{}
							b, err := banana.MarshalJSON()
							if err != nil {
								fmt.Println(err)
							}

							fmt.Println(string(b))

							err = json.Unmarshal(b, &pst)
							if err != nil {
								fmt.Println(err)
							}
						}
					}

				}

				return nil
			},
			Error: func(errf *events.ErrorFrame) error {
				return fmt.Errorf("error frame: %s: %s", errf.Error, errf.Message)
			},
		}

		seqScheduler := sequential.NewScheduler(con.RemoteAddr().String(), rscb.EventHandler)
		return events.HandleRepoStream(ctx, con, seqScheduler)
	},
}

func createSession(cctx *cli.Context) ([]byte, error) {
	xrpcc, err := cliutil.GetXrpcClient(cctx, false)
	if err != nil {
		return nil, err
	}
	handle := cctx.Args().Get(0)
	password := cctx.Args().Get(1)

	ses, err := comatproto.ServerCreateSession(context.TODO(), xrpcc, &comatproto.ServerCreateSession_Input{
		Identifier: handle,
		Password:   password,
	})
	if err != nil {
		return nil, err
	}

	return json.MarshalIndent(ses, "", "  ")
}
