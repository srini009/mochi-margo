# Margo
Margo is a library that provides Argobots bindings to the Mercury RPC
implementation.  See the following for more details about each project:

* https://collab.mcs.anl.gov/display/ARGOBOTS/Argobots+Home
* https://mercury-hpc.github.io/

Note that Margo should be compatible with any Mercury transport (NA plugin).  The documentation assumes the use of the NA SM (shared memory) plugin that is built into Mercury for simplicity.  This plugin is only valid for communication between processes on a single node.  See [Using Margo with other Mercury NA plugins](##using-margo-with-other-mercury-na-plugins) for information on other configuration options.

##  Dependencies

* mercury  (git clone --recurse-submodules https://github.com/mercury-hpc/mercury.git)
* argobots (git clone https://github.com/pmodels/argobots.git)
* abt-snoozer (git clone https://xgitlab.cels.anl.gov/sds/abt-snoozer)
* libev (e.g libev-dev package on Ubuntu or Debian)

### Recommended Mercury build options

* -DMERCURY_USE_CHECKSUMS:BOOL=OFF disables automatic checksumming of all
  Mercury RPC messages.  This reduces latency by removing a layer of
  integrity checking on communication.

## Building

Example configuration:

    ../configure --prefix=/home/pcarns/working/install \
        PKG_CONFIG_PATH=/home/pcarns/working/install/lib/pkgconfig \
        CFLAGS="-g -Wall"

## Running examples

The examples subdirectory contains:

* margo-example-client.c: an example client
* margo-example-server.c: an example server
* my-rpc.[ch]: an example RPC definition

The following example shows how to execute them.  Note that when the server starts it will display the address that the client can use to connect to it.


```
$ examples/margo-example-server na+sm://
# accepting RPCs on address "na+sm://13367/0"
Got RPC request with input_val: 0
Got RPC request with input_val: 1
Got RPC request with input_val: 2
Got RPC request with input_val: 3
Got RPC request to shutdown

$ examples/margo-example-client na+sm://13367/0
ULT [0] running.
ULT [1] running.
ULT [2] running.
ULT [3] running.
Got response ret: 0
ULT [0] done.
Got response ret: 0
ULT [1] done.
Got response ret: 0
ULT [2] done.
Got response ret: 0
ULT [3] done.
```

The client will issue 4 concurrent RPCs to the server and wait for them to
complete.

## Running tests

`make check`

## Using Margo with the other NA plugins

You can use either the CCI NA plugin or BMI NA plugin to use either the CCI or BMI library for remote communication.  See the [Mercury documentation](http://mercury-hpc.github.io/documentation/) for details and status.

### CCI

You must set the CCI_CONFIG environment variable to point to a valid CCI
configuration file.  You can use the following example and un-comment the
appropriate section for the transport that you wish to use.  Note that there
is no need to specify a port; Mercury will dictate a port for CCI to use if
needed.

```
[mercury]
# use this example for TCP
transport = tcp
interface = lo  # switch this to eth0 or an external hostname for non-localhost use

## use this example instead for shared memory
# transport = sm

## use this example instead for InfiniBand
# transport = verbs
# interface = ib0
```

You must then use addresses appropriate for your transport at run time when
executing Margo examples.  Examples for server "listening" addresses:

* cci+tcp://3344 # for TCP/IP, listening on port 3344
* cci+verbs://3344 # for InfiniBand, listening on port 3344
* cci+sm://1/1 # for shared memory, listening on CCI SM address 1/1

Examples for clients to specify to attach to the above:

* cci+tcp://localhost:3344 # for TCP/IP, assuming localhost use
* cci+verbs://192.168.1.78:3344 # for InfiniBand, note that you *must* use IP
  address rather than hostname
* cci+sm:///tmp/cci/sm/`hostname`/1/1 # note that this is a full path to local
  connection information.  The last portion of the path should match the
  address specified above

### BMI

We do not recommend using any BMI methods besides TCP.  It's usage is very similar to the CCI/TCP examples above, except that "bmi+" should be substituted for "cci+".
