package banyan_api_test

import (
    "fmt"
    "time"
    "testing"
    "banyan_api"
)

func TestKV(t *testing.T) {
    banyan_api.LogInit(banyan_api.LevelDebug, "by.log")
//    var err error
    cluster := banyan_api.NewClusterClient([]string{"10.10.105.5:10", "10.10.105.5:10025"})
    cli, err := cluster.GetBanyanClient("test", "api_test", 3000, 3)
    if (err != nil) {
        t.Fatalf("err: %s", err.Error())
    }

    var boolVal bool
    var intVal int
    var prefix, strVal, key, val string
    var mapVal map[string]string
    prefix = fmt.Sprintf("go_%d_", time.Now().Unix())
    key = prefix+"k0"
    val = "v0"
    err = cli.Set(key, val)
    if err != nil { 
        t.Fatalf("err: %s", err.Error())
    }
    strVal, err = cli.Get(key)
    if err != nil { 
        t.Fatalf("err: %s", err.Error())
    }
    if strVal != val {
        t.Errorf("val: %s", strVal)
    }
    strVal, err = cli.GetSet(key, "v1")
    if err != nil { 
        t.Fatalf("err: %s", err.Error())
    }
    if strVal != val {
        t.Errorf("val: %s", strVal)
    }
    strVal, err = cli.Get(key)
    if err != nil { 
        t.Fatalf("err: %s", err.Error())
    }
    if strVal != "v1" {
        t.Errorf("val: %s", strVal)
    }
    err = cli.Del(key)
    if err != nil { 
        t.Fatalf("err: %s", err.Error())
    }
    strVal, err = cli.Get(key)
    if err == nil || err.Error() != banyan_api.StatusNotFound { 
        t.Fatalf("err: %s", err.Error())
    }

    key = prefix + "k1"
    boolVal, err = cli.Exists(key)
    if err != nil {
        t.Fatalf("err: %s", err.Error())
    }
    if boolVal {
        t.Errorf("val: %t", boolVal)
    }
    intVal, err = cli.Incr(key, 888)
    if err != nil {
        t.Fatalf("err: %s", err.Error())
    }
    if intVal != 888 {
        t.Errorf("val: %d", intVal)
    }
    boolVal, err = cli.Exists(key)
    if err != nil {
        t.Fatalf("err: %s", err.Error())
    }
    if !boolVal {
        t.Errorf("val: %d", intVal)
    }

    key = prefix + "k2"
    boolVal, err = cli.SetBit(key, 123456, 1)
    if err != nil {
        t.Fatalf("err: %s", err.Error())
    }
    if boolVal {
        t.Errorf("val: %t", boolVal)
    }
    boolVal, err = cli.GetBit(key, 123456)
    if err != nil {
        t.Fatalf("err: %s", err.Error())
    }
    if !boolVal {
        t.Errorf("val: %d", intVal)
    }

    key3, key4, key5 := prefix + "k3", prefix + "k4", prefix + "k5"
    err = cli.MultiSet(key3, "v3", key4, "v4", key5, "v5")
    if err != nil {
        t.Fatalf("err: %s", err.Error())
    }
    mapVal, err = cli.MultiGet(key3, key4, key5)
    if err != nil {
        t.Fatalf("err: %s", err.Error())
    }
    val3, ok3 := mapVal[key3] 
    val4, ok4 := mapVal[key4] 
    val5, ok5 := mapVal[key5] 
    if !ok3 || !ok4 || !ok5 || val3 != "v3" || val4 != "v4" || val5 != "v5" {
        t.Errorf("val: %v", mapVal)
    }
    err = cli.MultiDel(key3, key4, key5)
    if err != nil {
        t.Fatalf("err: %s", err.Error())
    }
    // setx expire

    cluster.RetBanyanClient(cli)
    time.Sleep(time.Millisecond*200)
}

