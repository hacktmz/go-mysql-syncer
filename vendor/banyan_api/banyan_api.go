package banyan_api

import (
    "fmt"
    "net"
    "time"
//    "math/rand"
    "errors"
    "strings"
    "bytes"
    "strconv"
    "container/list"
    "sync"
)

type  ClusterClient struct {
    offset          int
    pools           []connPool
}

type connPool struct {
    down            bool
    fails           int
    lastFailTime    int64
    maxPoolSize     int
    maxConnSize     int
    currentConnSize int
    host            string
    mutex           sync.Mutex
    list            *list.List
}

type BanyanClient struct {
    ns          string
    tab         string
    proto       string
    timeout     int
    retries     int
    nread       int
    nwrite      int
    conn        net.Conn
    pool        *connPool
    cluster     *ClusterClient
    readBuf     []byte
    writeBuf    []byte
}

type BanyanResponse struct {
    cmd         string
    reps        []string
    status      string
    errmsg      string
    val         interface{}
    //valType     string
}

type mhmgetParam struct {
    keys        []string
    fields      []string
}

var intervalFail = 10
var maxFails = 3

func NewClusterClient(hosts []string) *ClusterClient {
    var l list.List
    d := net.Dialer{KeepAlive: time.Second*60}
    cluster := &ClusterClient{offset:0, pools: nil}
    for i := 0; i < len(hosts); i++ {
        j := 0
        m := sync.Mutex{}
        pool := connPool{maxPoolSize: 256, maxConnSize: 10*256, host: hosts[i], mutex: m, list: nil}
        pool.list = list.New()
        l.Init()
        for j < 16 {
            c, err := d.Dial("tcp", hosts[i])
            if err != nil {
                Error("%s", err.Error())
                break
            }
            //c.SetDeadline(time.Millisecond*3000) use
            j++
            l.PushBack(c)
        }
        if (j != 16) {
            pool.down = true
            for e := l.Front(); e != nil; e = e.Next() {
                c := e.Value.(net.Conn)
                c.Close()
            }
            // l.Init()
        } else {
            pool.currentConnSize = 16
            pool.list.PushBackList(&l)
        }
        cluster.pools = append(cluster.pools, pool)
    }
    go cluster.checkDownHost()
    return cluster
}

func (cluster ClusterClient) GetBanyanClient(ns string, tab string, timeout, retries int) (*BanyanClient, error) {
    bc := &BanyanClient{ns: ns, tab: tab, proto: "by", timeout: timeout, retries: retries, 
    nread: 0, nwrite: 0, conn: nil, pool: nil, cluster: &cluster, readBuf: []byte{}, writeBuf: []byte{}}
    conn, pool, err := cluster.getConn()
    if (err != nil) {
        return nil, errors.New("getConn() failed")
    } else {
        bc.conn = conn
        bc.pool = pool
        return bc, nil
    }
}

func (cluster *ClusterClient) getConn() (net.Conn, *connPool, error) {
    //src := rand.NewSource(time.Now().UnixNano())
    //r := rand.New(src)
    n := len(cluster.pools)
    if (n == 0) {
        return nil, nil, errors.New("getConn failed")
    }
    //x := r.Intn(n)
    x := cluster.offset
    for i := x; i < n; i++ {
        pool := &cluster.pools[i]
        c := pool.getConn()
        if c != nil {
            cluster.offset = i+1
            if (cluster.offset >= n) {
                cluster.offset = 0
            }
            return c, pool, nil
        }
    }
    for i := 0; i < x; i++ {
        pool := &cluster.pools[i]
        c := pool.getConn()
        if c != nil {
            cluster.offset = i+1
            if (cluster.offset >= n) {
                cluster.offset = 0
            }
            return c, pool, nil
        }
    }
    Error("getConn failed")
    return nil, nil, errors.New("getConn failed")
}

func (cluster *ClusterClient) RetBanyanClient(bc *BanyanClient) {
    if bc.conn != nil {
        bc.pool.returnConn(bc.conn, false)
    }
}

func (pool *connPool) getConn() (net.Conn) {
    var c net.Conn = nil
    flags := false
    Debug("get host:%s down:%t currentConnSize:%d MaxPoolSize:%d PoolSize:%d",
        pool.host, pool.down, pool.currentConnSize, pool.maxPoolSize, pool.list.Len())
    pool.mutex.Lock()
    if !pool.down {
        e := pool.list.Front()
        if e != nil {
            pool.list.Remove(e)
            c := e.Value.(net.Conn)
            pool.mutex.Unlock()
            return c
        }
        if pool.currentConnSize < pool.maxConnSize {
            flags = true
        } else {
            Warn("host:%s currentConnSize:%d more than maxConnSize:%d",
                pool.host, pool.currentConnSize, pool.maxConnSize)
        }
    }
    pool.mutex.Unlock()
    if (flags) {
        d := net.Dialer{KeepAlive: time.Second*60}
        conn, err := d.Dial("tcp", pool.host)
        if err == nil {
            c = conn
            pool.mutex.Lock()
            pool.currentConnSize++
            pool.mutex.Unlock()
        } else {
            Error("%s", err.Error())
        }
    }
    return c
}

func (pool *connPool) returnConn(c net.Conn, drop bool) {
    Debug("ret host:%s down:%t drop:%t conn:%d", pool.host, pool.down, drop, pool.list.Len())
    pool.mutex.Lock()
    if pool.down {
        c.Close()
        pool.mutex.Unlock()
        return
    }
    if drop {
        c.Close()
        now := time.Now().Unix()
        if (now - pool.lastFailTime > int64(intervalFail)) {
            pool.fails = 1
            pool.lastFailTime = now
        } else {
            pool.fails++
        }
        if pool.fails >= maxFails{
            pool.down = true
            pool.fails = 0
            pool.currentConnSize = 0
            var conn net.Conn
            for e := pool.list.Front(); e != nil; e = e.Next() {
                conn = e.Value.(net.Conn)
                conn.Close()
            }
            pool.list.Init()
        }
    } else {
        if (pool.list.Len() >= pool.maxPoolSize) {
            c.Close()
        } else {
            pool.list.PushBack(c)
        }
    }
    pool.mutex.Unlock()
}

func (cluster *ClusterClient) checkDownHost() {
    var err error
    var e *list.Element
    var c net.Conn
    var l list.List
    for {
        Debug("check down host")
        d := net.Dialer{KeepAlive: time.Second*60}
        n := len(cluster.pools)
        for i := 0; i < n; i++ {
            pool := &cluster.pools[i]
            if (pool.down) {
                pool.mutex.Lock()

                j := 0
                l.Init()
                for j < 8 {
                    c, err = d.Dial("tcp", pool.host)
                    if err != nil {
                        Error("%s", err.Error())
                        break
                    }
                    Debug("dial %s success", pool.host)
                    j++
                    l.PushBack(c)
                }
                if (l.Len() == 8) {
                    pool.currentConnSize = 8
                    pool.list.PushBackList(&l)
                    pool.down = false
                } else {
                    for e = l.Front(); e != nil; e = e.Next() {
                        c = e.Value.(net.Conn)
                        c.Close()
                    }
                }
                pool.mutex.Unlock()
            }
        }
        time.Sleep(time.Millisecond*100)
    }
}

func (bc *BanyanClient) request(cmd string, args ...interface{}) (*BanyanResponse, error) {
    opts := fmt.Sprintf("ns:%s,tab:%s,proto:%s", bc.ns, bc.tab, bc.proto)
    params := make([]string, 0, len(args))
    for _, arg := range args {
        switch v := arg.(type) {
        case nil:
            Error("args nil")
        case string:
            params = append(params, v)
        case []byte:
            params = append(params, string(v))
        case int:
            newarg := strconv.FormatInt(int64(v), 10)
            params = append(params, newarg)
        case int8:
            newarg := strconv.FormatInt(int64(v), 10)
            params = append(params, newarg)
        case int16:
            newarg := strconv.FormatInt(int64(v), 10)
            params = append(params, newarg)
        case int32:
            newarg := strconv.FormatInt(int64(v), 10)
            params = append(params, newarg)
        case int64:
            newarg := strconv.FormatInt(v, 10)
            params = append(params, newarg)
        case uint:
            newarg := strconv.FormatUint(uint64(v), 10)
            params = append(params, newarg)
        case uint8:
            newarg := strconv.FormatUint(uint64(v), 10)
            params = append(params, newarg)
        case uint16:
            newarg := strconv.FormatUint(uint64(v), 10)
            params = append(params, newarg)
        case uint32:
            newarg := strconv.FormatUint(uint64(v), 10)
            params = append(params, newarg)
        case uint64:
            newarg := strconv.FormatUint(v, 10)
            params = append(params, newarg)
        case float32:
            newarg := strconv.FormatFloat(float64(v), 'f', -1, 64)
            params = append(params, newarg)
        case float64:
            newarg := strconv.FormatFloat(v, 'f', -1, 64)
            params = append(params, newarg)
        case bool:
            if v {
                 params = append(params, "1")
            } else {
                 params = append(params, "0")
            }
        default:
        }
    }

    kvs := make([]string, 0, 2 * len(params) + 4)
    kvs = append(kvs,  fmt.Sprintf("%d", len(opts)), opts, fmt.Sprintf("%d", len(cmd)), cmd)
    for _, item := range params {
        kvs = append(kvs, fmt.Sprintf("%d", len(item)), item)
    }
    //wbuf := fmt.Sprintf("\n%s\n\n", strings.Join(kvs, "\n"))
    wbuf := fmt.Sprintf("%s\n\n", strings.Join(kvs, "\n"))
    bc.nread = 0
    bc.nwrite = 0
    bc.writeBuf = append(bc.writeBuf[:bc.nwrite], []byte(wbuf)...)
    bc.nwrite = len(wbuf)
    return bc.request2(cmd)
    br, err := bc.request2(cmd)
    if err != nil {
        Error("request2() failed:%s", err.Error())
        return br, err
    }
    if br.status != StatusOK {
        //    resErr := fmt.Sprintf("response status: %s", br.status)
        resErr := br.status
        Error("response error:%s", resErr)
        return br, errors.New(resErr)
    }
    return br, nil
}

func (bc *BanyanClient) request2(cmd string) (*BanyanResponse, error) {
    Debug("request: %s", strconv.QuoteToASCII(string(bc.writeBuf)))
    var flags bool
    var e error
    var reps []string
    reties := 0
    rbuf := make([]byte, 1024)
    for reties < bc.retries {
        flags = false
        if bc.conn == nil {
            conn, pool, err := bc.cluster.getConn()
            if (err != nil) {
                return nil, err
            }
            bc.nread = 0
            bc.conn = conn
            bc.pool = pool
        }
        if n, err := bc.conn.Write(bc.writeBuf[:bc.nwrite]); err != nil {
            reties++
            bc.pool.returnConn(bc.conn, true)
            bc.conn = nil
            Warn("conn %d write failed", reties)
            continue
        } else if n != bc.nwrite {
            reties++
            bc.pool.returnConn(bc.conn, true)
            bc.conn = nil
            Warn("conn %d write failed", reties)
            continue
        }

        for {
            n, err := bc.conn.Read(rbuf)
            if err != nil {
                reties++
                bc.pool.returnConn(bc.conn, true)
                bc.conn = nil
                Warn("conn %d read failed", reties)
                break
            }
            bc.readBuf = append(bc.readBuf[:bc.nread], rbuf[:n]...)
            bc.nread += n
            reps, e = parseResponseItem(bc.readBuf, bc.nread)
            if e != nil {
                break
            }
            if reps == nil {
                continue
            }
            flags = true
            break
        }
        if !flags {
            continue
        }
        break;
    }
    br := newBanyanResponse(cmd, reps)
    if !flags {
        Error("request: %s failed", strconv.QuoteToASCII(string(bc.writeBuf[:bc.nwrite])))
        return br, errors.New("request failed")
    }
    Debug("response: %s", strconv.QuoteToASCII(string(bc.readBuf[:bc.nread])))
    if br.status == StatusOK {
        err := br.parseResponseValue()
        return br, err
    } else if br.status == StatusNotFound || br.status == StatusBuffer {
        return br, errors.New(br.status)
    } else {
        return br, errors.New(br.errmsg)
    }
    err := br.parseResponseValue()
    return br, err
}

func parseResponseItem(rbuf []byte, n int) ([]string, error) {
    var spos, epos, pos int = 0, 0, 0
    reps := []string{}
    for {
        spos = epos
        pos = bytes.IndexByte(rbuf[spos:], byte('\n'))
        if pos == -1 {
            break
        }

        epos = spos + pos + 1
        line := string(rbuf[spos:epos])
        spos = epos
        line = strings.TrimSpace(line)
        if line == string("") {
            if len(reps) == 0 {
                continue
            } else {
                rbuf = rbuf[spos:]
                return reps, nil
            }
        }
        num, err := strconv.ParseUint(line, 0, 64)
        if err != nil {
            return nil, err
        }
        epos = spos + int(num)
        if epos > n {
            break
        }
        data := string(rbuf[spos:epos])
        reps = append(reps, data)
        spos = epos
        pos = bytes.IndexByte(rbuf[spos:], byte('\n'))
        if pos == -1 {
            break
        }
        epos = spos + pos + 1
    }

    return nil, nil
}

var StatusOK = string("ok")
var StatusError = string("error")
var StatusNotFound = string("not_found")
var StatusBuffer = string("buffer")

var retNone = map[string]bool{
    "ping": true, "quit": true,
}

var retInt = map[string]bool{
    "set": true, "del": true, "setx": true, "expire": true, "incr": true, "exists": true, 
    "getbit": true, "setbit": true, "hset": true, "hdel": true, "hsize": true, "hincr": true, 
    "hexists": true, "hclear": true, "multi_hset": true, "multi_hdel": true, "zset": true, "zdel": true, 
    "zsize": true, "zincr": true, "zcount": true, "zclear": true, "zremrangebyrank": true, 
    "zexists": true, "multi_zset": true, "multi_zdel": true,
    "qsize": true, "qpush": true, "qclear": true,
}

var retBin = map[string]bool{
    "get": true, "getset": true, "hget": true, "zget": true,
}

var retList = map[string]bool{
    "keys": true, "rkeys": true,
    "hkeys": true, "hrkeys": true, "hlist": true, "hrlist": true,
    "zlist": true, "zrlist": true,
    "qlist": true, "qrlist": true, "qpop": true, "qslice": true, "qrange": true, 
    "info": true,
}

var retIntList = map[string]bool{
    "multi_set": true, "multi_del": true,
}

var retMapString = map[string]bool{
    "scan": true, "rscan": true, "multi_get": true, 
    "hscan": true, "hrscan": true, "hgetall": true, "multi_hget": true,
}

var retMapInt = map[string]bool{
    "zgetall": true, "zrange": true, "zrrange": true, "zscan": true, "zrscan": true, "multi_zget": true,
}

var retTable = map[string]bool{
    "mhmget": true,
}

func newBanyanResponse(cmd string, reps []string) (*BanyanResponse) {
    Debug("reps:%v", reps)
    var ok bool
    br := &BanyanResponse{cmd: cmd, reps: reps, status: "error", errmsg: "", val: nil}
    if reps == nil || len(reps) == 0 {
        br.status =  StatusError
    } else {
        switch reps[0]{
        case  StatusOK:
            br.status = StatusOK
        case StatusNotFound:
            br.status = StatusNotFound
        case StatusBuffer:
            br.status = StatusBuffer 
        default:
            br.status = StatusError
            if len(reps) > 1 {
                br.errmsg = reps[1]
            }
        }
    }

    if  _, ok = retNone[br.cmd]; ok {
        br.val = ""
        return br
    } 
    if _, ok = retInt[br.cmd]; ok {
        val := -1
        br.val = val
        return br
    }
    if _, ok = retBin[br.cmd]; ok {
        val := ""
        br.val = val
        return br
    }
    if _, ok = retList[br.cmd]; ok {
        val := []string{}
        br.val = val
        return br
    }
    if _, ok = retMapString[br.cmd]; ok {
        val := map[string]string{"":""}
        br.val = val
        return br
    }
    if _, ok = retMapInt[br.cmd]; ok {
        val := map[string]int{"":0}
        br.val = val
        return br
    }
    /*if _, ok = retIntList[br.cmd]; ok {
        val := map[string]{"":}
        br.val = val
        return nil
    }*/

    return br
}

func (br *BanyanResponse) parseResponseValue() (error) {
    var ok bool
    if  _, ok = retNone[br.cmd]; ok {
        br.val = ""
        return nil
    } 
    if _, ok = retInt[br.cmd]; ok {
        //val, _ :=  strconv.Atoi(br.reps[1])
        val, _ := strconv.ParseInt(br.reps[1], 10, 0)
        br.val = int(val)
        return nil
    }
    if _, ok = retBin[br.cmd]; ok {
        br.val =  br.reps[1]
        return nil
    }
    if _, ok = retList[br.cmd]; ok {
        br.val =  br.reps[1:]
        return nil
    }
    if _, ok = retMapString[br.cmd]; ok {
        l := len(br.reps)
        if l%2 == 0 {
            return errors.New("wrong number of response items")
        }
        val := make(map[string]string, (l-1)/2)
        for i := 1; i < l; i += 2 {
            val[br.reps[i]] = br.reps[i+1]
        }
        br.val = val
        return nil
    }
    if _, ok = retMapInt[br.cmd]; ok {
        l := len(br.reps)
        if l/2 == 0 {
            return errors.New("wrong number of response items")
        }
        val := make(map[string]int, (l-1)/2)
        for i := 1; i < l; i += 2 {
            x, _ := strconv.ParseInt(br.reps[i+1], 10, 0)
            val[br.reps[i]] = int(x)
        }
        br.val = val
        return nil
    }
    if _, ok = retIntList[br.cmd]; ok {
        val, _ := strconv.ParseInt(br.reps[1], 10, 0)
        br.val = val
        return nil
    }

    return errors.New("parse response value failed")
}

func (br *BanyanResponse) Val() interface{} {
    return br.val
}

/******kv******/
func (bc *BanyanClient) Get(key string) (string, error) {
    br, err := bc.request("get", key)
    return br.Val().(string), err
}

func (bc *BanyanClient) Set(key, val string) (error) {
    _, err := bc.request("set", key, val)
    return err
}

func (bc *BanyanClient) Setx(key, val string, ttl uint32) (error) {
    _, err := bc.request("setx", key, ttl)
    return err
}

func (bc *BanyanClient) Expire(key string, ttl uint32) (error) {
    _, err := bc.request("expire", key, ttl)
    return err
}

func (bc *BanyanClient) GetSet(key, val string) (string, error) {
    br, err := bc.request("getset", key, val)
    return br.Val().(string), err
}

func (bc *BanyanClient) Del(key string) (error) {
    _, err := bc.request("del", key)
    return err
}

func (bc *BanyanClient) Incr(key string, n int) (int, error) {
    br, err := bc.request("incr", key, n)
    return br.Val().(int), err
}

func (bc *BanyanClient) Exists(key string) (bool, error) {
    br, err := bc.request("exists", key)
    if err != nil {
        return false, err
    }
    flags := true
    if (br.Val().(int) != 1) {
        flags = false
    }
    return flags, nil
}

func (bc *BanyanClient) GetBit(key string, n uint32) (bool, error) {
    br, err := bc.request("getbit", key, n)
    if err != nil {
        return false, err
    }
    flags := true
    if (br.Val().(int) != 1) {
        flags = false
    }
    return flags, nil
}

func (bc *BanyanClient) SetBit(key string, n uint32, bit uint32) (bool, error) {
    br, err := bc.request("setbit", key, n, bit)
    if err != nil {
        return false, err
    }
    flags := true
    if (br.Val().(int) != 1) {
        flags = false
    }
    return flags, nil
}

func (bc *BanyanClient) Keys(start, end string, limit uint32) ([]string, error) {
    br, err := bc.request("keys", start, end, limit)
    return br.Val().([]string), err
}

func (bc *BanyanClient) Rkeys(start, end string, limit uint32) ([]string, error) {
    br, err := bc.request("rkeys", start, end, limit)
    return br.Val().([]string), err
}

func (bc *BanyanClient) Scan(start, end string, limit uint32) (map[string]string, error) {
    br, err := bc.request("scan", start, end, limit)
    return br.Val().(map[string]string), err
}

func (bc *BanyanClient) Rscan(start, end string, limit uint32) (map[string]string, error) {
    br, err := bc.request("rscan", start, end, limit)
    return br.Val().(map[string]string), err
}

func (bc *BanyanClient) MultiGet(keys ...string) (map[string]string, error) {
    args := make([]interface{}, len(keys))
    for i, key := range keys {
        args[i] = key
    }
    br, err := bc.request("multi_get", args...)
    return br.Val().(map[string]string), err
}

func (bc *BanyanClient) MultiSet(kvs ...string) (error) {
    args := make([]interface{}, len(kvs))
    for i, pair := range kvs {
        args[i] = pair
    }
    _, err := bc.request("multi_set", args...)
    return err
}

func (bc *BanyanClient) MultiDel(keys ...string) (error) {
    args := make([]interface{}, len(keys))
    for i, key := range keys {
        args[i] = key
    }
    _, err := bc.request("multi_del", args...)
    return err
}

/******hash******/
func (bc *BanyanClient) Hsize(key string) (int, error) {
    br, err := bc.request("hsize", key)
    return br.Val().(int), err
}

func (bc *BanyanClient) Hget(key, field string) (string, error) {
    br, err := bc.request("hget", key, field)
    return br.Val().(string), err
}

func (bc *BanyanClient) Hset(key, field, val string) (error) {
    _, err := bc.request("hset", key, field, val)
    return err
}

func (bc *BanyanClient) Hdel(key, field string) (error) {
    _, err := bc.request("hdel", key, field)
    return err
}

func (bc *BanyanClient) Hincr(key, field string, incr int) (int, error) {
    br, err := bc.request("hincr", key, incr)
    return br.Val().(int), err
}

func (bc *BanyanClient) Hexists(key, field string) (bool, error) {
    br, err := bc.request("hexists", key, field)
    if err != nil {
        return false, err
    }
    var flags bool = true
    if (br.Val().(int) != 1) {
        flags = false
    }
    return flags, nil
}

func (bc *BanyanClient) Hgetall(key string) (map[string]string, error) {
    br, err := bc.request("hgetall", key)
    return br.Val().(map[string]string), err
}

func (bc *BanyanClient) Hclear(key string) (error) {
    _, err := bc.request("hclear", key)
    return err
}

// (start, end]
func (bc *BanyanClient) Hscan(key, start, end string, limit int) (map[string]string, error) {
    br, err := bc.request("hscan", key, start, end, limit)
    return br.Val().(map[string]string), err
}

func (bc *BanyanClient) Hrscan(key, start, end, limit int) (map[string]string, error) {
    br, err := bc.request("hrscan", key, start, end, limit)
    return br.Val().(map[string]string), err
}

func (bc *BanyanClient) Hkeys(key, start, end, limit int) ([]string, error) {
    br, err := bc.request("hkeys", key, start, end, limit)
    return br.Val().([]string), err
}

func (bc *BanyanClient) Hrkeys(key, start, end, limit int) ([]string, error) {
    br, err := bc.request("hrkeys", key, start, end, limit)
    return br.Val().([]string), err
}

func (bc *BanyanClient) Hlist(start, end, limit int) ([]string, error) {
    br, err := bc.request("hlist", start, end, limit)
    return br.Val().([]string), err
}

func (bc *BanyanClient) Hrlist(start, end, limit int) ([]string, error) {
    br, err := bc.request("hrlist", start, end, limit)
    return br.Val().([]string), err
}

func (bc *BanyanClient) MultiHget(key string, fields ...string) (map[string]string, error) {
    args := make([]interface{}, 0, len(fields)+1)
    args = append(args, key)
    for _, item := range fields {
        args = append(args, item)
    }
    br, err := bc.request("multi_hget", args...)
    return br.Val().(map[string]string), err
}

func (bc *BanyanClient) MultiHset(key string, kvs map[string]string) (error) {
    args := make([]interface{}, 0, 2*len(kvs)+1)
    args = append(args, key)
    for k, v := range kvs {
        args = append(args, k, v)
    }
    _, err := bc.request("multi_hset", args...)
    return err
}

func (bc *BanyanClient) MultiHdel(key string, fields ...string) (error) {
    args := make([]interface{}, 0, len(fields)+1)
    args = append(args, key)
    for _, item := range fields {
        args = append(args, item)
    }
    _, err := bc.request("multi_hdel", args...)
    return err
}

func (bc *BanyanClient) Mhmget(keys, fields []string) (map[string]map[string]string, error) {
    opts := fmt.Sprintf("ns:%s,tab:%s,proto:%s,nkey:%d,nfield:%d", bc.ns, bc.tab, bc.proto, len(keys), len(fields))
    args := make([]string, 0, len(keys) + len(fields) + 2)
    args = append(args, opts, "mhmget")
    args = append(args, keys...)
    args = append(args, fields...)
    kvs := make([]string, 0, 2*len(args))
    for _, item := range args {
        kvs = append(kvs, fmt.Sprintf("%d", len(item)), item)
    }
    wbuf := fmt.Sprintf("%s\n\n", strings.Join(kvs, "\n"))
    bc.nread = 0
    bc.nwrite = 0
    bc.writeBuf = append(bc.writeBuf[:bc.nwrite], []byte(wbuf)...)
    bc.nwrite = len(wbuf)
    br, err := bc.request2("mhmget")
    return br.Val().(map[string]map[string]string), err
}

/******zset******/
func (bc *BanyanClient) Zsize(key string) (int, error) {
    br, err := bc.request("zsize", key)
    return br.Val().(int), err
}

func (bc *BanyanClient) Zget(key, member string) (string, error) {
    br, err := bc.request("zget", key, member)
    return br.Val().(string), err
}

func (bc *BanyanClient) Zset(key, member string, score int) (int, error) {
    br, err := bc.request("zset", key, member, score)
    return br.Val().(int), err
}

func (bc *BanyanClient) Zdel(key, member string) (error) {
    _, err := bc.request("zdel", key, member)
    return err
}

func (bc *BanyanClient) Zincr(key, member string, incr int) (int, error) {
    br, err := bc.request("zincr", key, member, incr)
    return br.Val().(int), err
}

func (bc *BanyanClient) Zexists(key, member string) (bool, error) {
    br, err := bc.request("zexists", key, member)
    var flags bool = true
    if (br.Val().(int) != 1) {
        flags = false
    }
    return flags, err
}

func (bc *BanyanClient) Zcount(key string) (int, error) {
    br, err := bc.request("zcount", key)
    return br.Val().(int), err
}

func (bc *BanyanClient) Zgetall(key string) (map[string]int, error) {
    br, err := bc.request("zgetall", key)
    return br.Val().(map[string]int), err
}

func (bc *BanyanClient) Zclear(key string) (error) {
    _, err := bc.request("zclear", key)
    return err
}

func (bc *BanyanClient) Zremrangebyrank(key string, star, end int) (int, error) {
    br, err := bc.request("zremrangebyrank", key, star, end)
    return br.Val().(int), err
}

func (bc *BanyanClient) Zrange(key string, offset, limit int) (map[string]int, error) {
    br, err := bc.request("zrange", key, offset, limit)
    return br.Val().(map[string]int), err
}

func (bc *BanyanClient) Zrrange(key string, offset, limit int) (map[string]int, error) {
    br, err := bc.request("zrrange", key, offset, limit)
    return br.Val().(map[string]int), err
}

func (bc *BanyanClient) Zscan(key, member string, start, end, limit int) (map[string]int, error) {
    br, err := bc.request("zscan", key, member, start, end, limit)
    return br.Val().(map[string]int), err
}

func (bc *BanyanClient) Zrscan(key, member string, start, end, limit int) (map[string]int, error) {
    br, err := bc.request("zrscan", key, member, start, end, limit)
    return br.Val().(map[string]int), err
}

func (bc *BanyanClient) Zlist(start, end string, limit int) ([]string, error) {
    br, err := bc.request("zlist", start, end, limit)
    return br.Val().([]string), err
}

func (bc *BanyanClient) Zrlist(start, end string, limit int) ([]string, error) {
    br, err := bc.request("zrlist", start, end, limit)
    return br.Val().([]string), err
}

func (bc *BanyanClient) MultiZget(key string, members...string) (map[string]int, error) {
    br, err := bc.request("multi_zget", key, members)
    return br.Val().(map[string]int), err
}

func (bc *BanyanClient) MultiZset(key string, kvs map[string]int) (error) {
    args := make([]interface{}, 0, 2*len(kvs)+1)
    args = append(args, key)
    for k, v := range kvs {
        args = append(args, k, v)
    }
    _, err := bc.request("multi_zset", args...)
    return err
}

func (bc *BanyanClient) MultiZdel(key string, members ...string) (error) {
    args := make([]interface{}, 0, len(members)+1)
    args = append(args, key)
    for _, item := range members {
        args = append(args, item)
    }
    _, err := bc.request("multi_zdel", args...)
    return err
}

/******queue******/
func (bc *BanyanClient) Qsize(key string) (int, error) {
    br, err := bc.request("qsize", key)
    return br.Val().(int), err
}

func (bc *BanyanClient) Qpush(key string, members... string) (int, error) {
    args := make([]interface{}, 0, len(members)+1)
    args = append(args, key)
    for _, item := range members {
        args = append(args, item)
    }
    br, err := bc.request("qpush", args...)
    return br.Val().(int), err
}

func (bc *BanyanClient) Qpop(key string) (string, error) {
    br, err := bc.request("qpop", key)
    val := br.Val().([]string)
    return val[0], err
}

/*func (bc *BanyanClient) Qpop(key string, limit int) ([]string, error) {
    br, err := bc.request("qpop", key, limit)
    return br.Val().([]string), err
}*/

func (bc *BanyanClient) Qslice(key string, start, end int) ([]string, error) {
    br, err := bc.request("qslice", key, start, end)
    return br.Val().([]string), err
}

func (bc *BanyanClient) Qrange(key string, offset, limit int) ([]string, error) {
    br, err := bc.request("qrange", key, offset, limit)
    return br.Val().([]string), err
}

func (bc *BanyanClient) Qclear(key string) (error) {
    _, err := bc.request("qclear", key)
    return err
}

func (bc *BanyanClient) Qlist(start, end string, limit int) ([]string, error) {
    br, err := bc.request("qlist", start, end, limit)
    return br.Val().([]string), err
}

func (bc *BanyanClient) Qrlist(start, end string, limit int) ([]string, error) {
    br, err := bc.request("qrlist", start, end, limit)
    return br.Val().([]string), err
}
