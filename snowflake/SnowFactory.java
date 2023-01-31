package snowflake;

/** 雪花算法--唯一编号 */
public class SnowflakeFactory {

    /** 默认的序列号 bit 占位 */
    public static final int DEFAULT_SERIAL_BITS = 12;
    /** 默认的机器号 bit 占位 */
    public static final int DEFAULT_WORKER_BITS = 10;
    /** 起始时间戳 2023-01-25 16:27:32*/
    public static final long BEGIN_TIMESTAMP = 1674635252000L;

    /** 序列号 bit 占位 */
    private final int serialBits;
    /** 机器号 bit 占位 */
    private final int workerBits;
    /** 机器号 */
    private final int workerId;
    /** 最大序列号 */
    private final int maxSerial;
    /** 时间戳左移填充 */
    private final int timestampLeftRvm;
    /** 记录上次生成的ID */
    private long prevId;

    public SnowflakeFactory(int workerId){
        this(DEFAULT_SERIAL_BITS, DEFAULT_WORKER_BITS, workerId);
    }

    public SnowflakeFactory(int serialBits, int workerBits, int workerId) {
        if ((serialBits + workerBits) > 28) {
            throw new IllegalArgumentException("The sum of serialBits and workerBits cannot be greater than 28");
        }
        this.workerBits = workerBits;
        int maxWorkId = (int) (-1L ^ (-1L << this.workerBits));
        if (workerId > maxWorkId) {
            throw new IllegalArgumentException("The workerId cannot be greater than " + maxWorkId);
        }
        this.serialBits = serialBits;
        this.timestampLeftRvm = this.serialBits + this.workerBits;
        this.maxSerial = (int) (-1L ^ (-1L << serialBits));
        this.workerId = workerId;
        // 额，第一个序列号就浪费了
        this.prevId = (0L << timestampLeftRvm
                | workerId << serialBits
                | 0L);
    }

    /** 同步生成本机器上全局唯一编码 */
    public synchronized long nextId() throws InterruptedException{
        // 起始时间戳非 1970年那个，而是自定义的
        long now = currentTimeMillis();
        // 上次的序列号
        int prevSerial = (int) (maxSerial & prevId);
        // 上次的时间戳
        long preMoment = prevId >>> timestampLeftRvm;
        // 设备的时间被回拨了
        if (now < preMoment || now < 0) {
            now = waitUntilNnMoment(preMoment);
        }
        // 相同时刻下，序列号递增
        if (now == preMoment) {
            if (prevSerial >= maxSerial) {
                prevSerial = 0;
                // 等待直到下一个时刻
                now = waitUntilNnMoment(preMoment);
            } else {
                prevSerial++;
            }
        }
        // 时间前进了
        else {
            prevSerial = 0;
        }
        prevId = (now << timestampLeftRvm
                | workerId << serialBits
                | prevSerial);
        return prevId;
    }

    /** 回拨时间小于5ms，等待2倍ms，直到大于前一毫秒值 */
    private long waitUntilNnMoment(final long preMoment) throws InterruptedException {
        long nowMoment = currentTimeMillis();
        // 机器时间被回拨的时候
        if (nowMoment < preMoment) {
            long offset = preMoment - nowMoment;
            if (offset < 5L) {
                try {
                    // TODO: wait/sleep/自旋 待考虑
                    // wait 的话，会释放同步锁，即使有线程来了, 大概率还是会受机器时钟回拨影响的
                    // sleep 独自占用同步锁, 直到回拨矫正, 或者回拨异常
                    // 自旋 8ms CPU咋样不确定。。
                    Thread.sleep(offset << 1L);
                    nowMoment = currentTimeMillis();
                    // 还是处于回拨状态，直接抛异常
                    if (nowMoment < preMoment) {
                        throwClockBackwardsEx(nowMoment);
                    }
                } catch (InterruptedException e) {
                    throw e;
                }
            } else {
                throwClockBackwardsEx(nowMoment);
            }
        }
        // 序列号最大的时候
        else if (nowMoment == preMoment) {
            // 最多自旋1ms
            while(nowMoment == preMoment){
                nowMoment = currentTimeMillis();
            }
        }
        return nowMoment;
    }

    /** 距离 BEGIN_TIMESTAMP 的毫秒数 */
    private long currentTimeMillis() {
        return System.currentTimeMillis() - BEGIN_TIMESTAMP;
    }

    /** 机器的时钟回拨抛出异常 */
    private void throwClockBackwardsEx(long timestamp) {
        throw new RuntimeException(
                "The time of the device is called back, current timeMillis: " + (timestamp + BEGIN_TIMESTAMP));
    }

    public static void main(String[] args) {
        SnowflakeFactory snowFactory = new SnowflakeFactory(1);
        long lastId = -1L;
        try {
            for (int i = 0; i < 10; i++) {
                long nowId = snowFactory.nextId();
                System.out.println(nowId);
                // 不是递增的序列直接抛异常
                if (nowId <= lastId) {
                    throw new RuntimeException("more than");
                }
                lastId = nowId;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}