import Control.Applicative (pure)
import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TChan (TChan, newTChan, readTChan, writeTChan,
                                     isEmptyTChan)
import Control.Exception (try, SomeException)
import Control.Monad (forever, forM_, liftM)
import qualified Data.ByteString as BS
import Data.Function (on)
import qualified Data.Map as M
import Data.Maybe (catMaybes)
import qualified Network.Socket as S
import qualified Network.Socket.ByteString as SB
import System.Environment (getArgs)
import System.IO (openFile, hSetBuffering, hGetLine, hPutStrLn,
                  IOMode (..), BufferMode(..), Handle)
import System.Timeout (timeout)
import System.Time (getClockTime, ClockTime(TOD))
import Text.Printf (printf)


data Message = NewRxConnection S.SockAddr
             | LostRxConnection
             | NewTxConnection
             | LostTxConnection
             | DataReceived Int
             | DataSent Int
             | Tick
               deriving (Show)

data TxConnMessage = TxEstablished
                   | TxLost
                   | TxFailed

data LogMessage = LogString String | LogStats BinStats deriving (Show)

data Stats = Stats
    { sRxBytes :: !Integer
    , sTxBytes :: !Integer
    } deriving Show

data Bin = Bin
    { bTicks     :: !Int
    , bMaxTicks  :: !Int
    , bPrevStats :: !Stats
    } deriving Show

data BinStats = BinStats
    { bsTicks   :: !Int
    , bsRxBytes :: !Integer
    , bsTxBytes :: !Integer
    } deriving Show

blockSize = 2^16
defaultTickPeriodUs = 1000000 :: Int
defaultTicksPerBin = [1, 20, 60, 600, 3600]

main :: IO ()
main = S.withSocketsDo $ do
    args <- getArgs
    if length args /= 4 then
        putStrLn "Usage: Main [tx_threads] [local_port] [remote_ip] [remote_port]"
    else do
        let threads   = read $ args !! 0
            localPort = read $ args !! 1
            peerPort  = read $ args !! 3
        peerAddr <- S.inet_addr $ args !! 2
        dataChan <- atomically newTChan
        logChan  <- atomically newTChan
        forkIO $ doRx dataChan localPort
        forkIO $ doTx dataChan (S.SockAddrInet (fromIntegral peerPort) peerAddr)
                      threads
        forkIO $ gatherStats dataChan logChan
        writeLogs logChan
        return ()


writeLogs :: TChan LogMessage -> IO ()
writeLogs chan = do
    mainLog <- open "main.log"
    loop mainLog M.empty
  where
    open name = do
        h <- openFile name AppendMode
        hSetBuffering h NoBuffering
        return h

    loop mainLog tickLogs = do
        m  <- atomically $ readTChan chan
        ut <- liftM fmtClock getClockTime
        case m of
            LogString s -> do
                putStrLn s
                hPutStrLn mainLog $ ut ++ " " ++ s
                loop mainLog tickLogs
            LogStats bw@(BinStats ticks rx tx) -> do
                let rx' = 8 * rx `div` (fromIntegral ticks)
                    tx' = 8 * tx `div` (fromIntegral ticks)
                (tickLogs', h) <- getOrOpen tickLogs ticks
                putStrLn $ fmtBw bw
                hPutStrLn h (ut ++ " " ++ show rx' ++ " " ++ show tx')
                loop mainLog tickLogs'

    getOrOpen tickLogs ticks =
        case M.lookup ticks tickLogs of
            Just h -> return (tickLogs, h)
            Nothing -> do h <- open $ "bw" ++ show ticks ++ ".log"
                          return (M.insert ticks h tickLogs, h)

    fmtClock (TOD s ps) = printf "%d.%06d" s (ps `div` 10^6)

    fmtBw s = let ticks = bsTicks s
                  fmt x = formatBw (8 * x `div` fromIntegral ticks)
              in (fmt $ bsRxBytes s) ++ " " ++ (fmt $ bsTxBytes s)

gatherStats :: TChan Message -> TChan LogMessage -> IO ()
gatherStats chan logChan = do
    forkIO $ tick
    let stats = Stats 0 0
    let bins = map (\n -> Bin 0 n stats) defaultTicksPerBin
    processMessages stats bins
  where
    tick = forever $ do
        threadDelay defaultTickPeriodUs
        atomically $ writeTChan chan Tick

    processMessages stats bins = do
        m <- atomically $ readTChan chan
        case m of
            DataReceived n -> let stats' = stats {sRxBytes = sRxBytes stats +
                                                         fromIntegral n}
                              in processMessages stats' bins
            DataSent n     -> let stats' = stats {sTxBytes = sTxBytes stats +
                                                         fromIntegral n}
                              in processMessages stats' bins
            Tick           -> let (bins', diffs) = unzip $ map (updateBin stats) bins
                                  diffs' = catMaybes diffs
                              in printStats diffs' >> processMessages stats bins'
            m              -> do atomically $ writeTChan logChan (LogString $ show m)
                                 processMessages stats bins
    updateBin stats bin =
        let ticks' = (bTicks bin + 1) `rem` bMaxTicks bin
            bin'  = bin {bTicks = ticks'}
            bin'' = bin' {bPrevStats = stats}
            bs = BinStats { bsTicks   = bMaxTicks bin
                          , bsRxBytes = ((-) `on` (sRxBytes . bPrevStats)) bin'' bin
                          , bsTxBytes = ((-) `on` (sTxBytes . bPrevStats)) bin'' bin
                          }
        in case bTicks bin' of
            0 -> (bin'', Just bs)
            _ -> (bin', Nothing)
    printStats stats = forM_ stats $ \s ->
        atomically $ writeTChan logChan $ LogStats s



doRx :: TChan Message -> Int -> IO ()
doRx chan portno = do
    sock <- setup portno
    process sock
  where
    setup portno = do
        sock <- S.socket S.AF_INET S.Stream S.defaultProtocol
        S.setSocketOption sock S.ReuseAddr 1
        S.bindSocket sock $ S.SockAddrInet (fromIntegral portno) 0
        S.listen sock 10
        return sock
    process sock = forever $ do
        (s, addr) <- S.accept sock
        sendMsg $ NewRxConnection addr
        forkIO $ rx s
    rx sock = do
        d <- SB.recv sock blockSize
        case BS.length d of
          0 -> sendMsg LostRxConnection >> S.sClose sock
          n -> (sendMsg $ DataReceived n) >> rx sock
    sendMsg m = atomically $ writeTChan chan $ m


doTx :: TChan Message -> S.SockAddr -> Int -> IO ()
doTx chan addr num = do
    chan' <- atomically newTChan
    loop 0 chan'
  where
    loop nRunning chan' = do
        let delay = if nRunning < num then 50000 else -1
        m <- timeout delay $ atomically $ readTChan chan'
        case m of
            Just TxLost -> do
                atomically $ writeTChan chan LostTxConnection
                loop (nRunning - 1) chan'
            Just TxFailed -> do
                loop (nRunning - 1) chan'
            Just TxEstablished -> do
                atomically $ writeTChan chan NewTxConnection
            Nothing -> do
                forkIO $ txConnection chan'
                loop (nRunning + 1) chan'
    txConnection chan' = do
        sock <- S.socket S.AF_INET S.Stream S.defaultProtocol
        r <- try (S.connect sock addr) :: IO (Either SomeException ())
        case r of
            Right _ -> floodSocket chan' sock
            Left _  -> atomically $ writeTChan chan' TxFailed
        S.sClose sock

    floodSocket chan' sock = do
        let bs = BS.replicate blockSize 0
        r <- try (SB.send sock bs) :: IO (Either SomeException Int)
        case r of
            Right n -> do
                atomically $ writeTChan chan $ DataSent n
                floodSocket chan' sock
            Left _  -> atomically $ writeTChan chan' $ TxLost


suffixNames :: [(Integer, String)]
suffixNames = zip (map (1000^) [0..]) (map pure "BKMGT")

formatBw :: Integer -> String
formatBw i = let (mult, name) = findSuffix
                 val = fromIntegral i / fromIntegral mult :: Double
             in printf "%6.1f" val ++ name
  where
    findSuffix = last $ head suffixNames :
                        takeWhile ((i > ) .(*10) . fst) suffixNames