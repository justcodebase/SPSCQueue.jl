using PosixIPC.Queues
using PosixIPC.Queues.SPSC
using PosixIPC.SharedMemory

storage_size = 1000 # bytes

shm = shm_open(
    "spscqueue_jl_to_cpp",
    oflag=Base.Filesystem.JL_O_CREAT |
            Base.Filesystem.JL_O_RDWR |
            Base.Filesystem.JL_O_TRUNC,
    mode=0o666,
    size=storage_size
)
println("storage_size=$(storage_size)")
println("shm_size=$(shm.size)")

storage = SPSCStorage(shm.ptr, shm.size)

# create variable-element size SPSC queue
queue = SPSCQueueVar(storage)

# spawn producer and consumer threads, pin them to cores 3 and 5
iterations = 1_000_000

# 16 bytes message
size = 16
data = Int64[0, 1]
GC.@preserve data begin
    data_ptr = pointer(data)
    msg = Message(data_ptr, size)
    for counter in 1:iterations
        # store counter value in message
        unsafe_store!(data_ptr, counter)
        unsafe_store!(data_ptr + 8, 10counter)

        println("> sending $counter")

        if !enqueue!(queue, msg)
            println("queue full")
        end

        sleep(0.05)
    end
end
