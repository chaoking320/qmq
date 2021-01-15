/*
 * Copyright 2018 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package qunar.tc.qmq.concurrent;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.monitor.QMon;

import java.lang.reflect.Field;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaohui.yu
 * 16/4/8
 */
public class ActorSystem {
    private static final Logger LOG = LoggerFactory.getLogger(ActorSystem.class);

    private static final int DEFAULT_QUEUE_SIZE = 10000;
    // 内部维护的是一个ConcurrentMap，key即PullMessageWorker里的subject+group
    private final ConcurrentMap<String, Actor> actors;
    // 执行actor的executor
    private final ThreadPoolExecutor executor;
    private final AtomicInteger actorsCount;
    private final String name;

    public ActorSystem(String name) {
        this(name, Runtime.getRuntime().availableProcessors() * 4, true);
    }

    public ActorSystem(String name, int threads, boolean fair) {
        this.name = name;
        this.actorsCount = new AtomicInteger();
        // 这里根据fair参数初始化一个优先级队列作为executor的参数，处理关于前言里说的"老消息"的情况
        BlockingQueue<Runnable> queue = fair ? new PriorityBlockingQueue<>() : new LinkedBlockingQueue<>();
        this.executor = new ThreadPoolExecutor(threads, threads, 60, TimeUnit.MINUTES, queue, new NamedThreadFactory("actor-sys-" + name));
        this.actors = Maps.newConcurrentMap();
        QMon.dispatchersGauge(name, actorsCount::doubleValue);
        QMon.actorSystemQueueGauge(name, () -> (double) executor.getQueue().size());
    }

    // PullMessageWorker调用的就是这个方法
    public <E> void dispatch(String actorPath, E msg, Processor<E> processor) {
        // 取得actor
        Actor<E> actor = createOrGet(actorPath, processor);
        // 在后文Actor定义里能看到，actor内部维护一个queue，这里actor仅仅是offer(msg)
        actor.dispatch(msg);
        // 执行调度
        schedule(actor, true);
    }

    // 无消息时，则会挂起
    public void suspend(String actorPath) {
        Actor actor = actors.get(actorPath);
        if (actor == null) return;

        actor.suspend();
    }

    // 有消息则恢复，可以理解成线程的"就绪状态"
    public void resume(String actorPath) {
        Actor actor = actors.get(actorPath);
        if (actor == null) return;

        actor.resume();
        // 立即调度，可以留意一下那个false
        // 当actor是"可调度状态"时，这个actor是否能调度是取决于actor的queue是否有消息
        schedule(actor, false);
    }

    private <E> Actor<E> createOrGet(String actorPath, Processor<E> processor) {
        Actor<E> actor = actors.get(actorPath);
        if (actor != null) return actor;

        Actor<E> add = new Actor<>(this.name, actorPath, this, processor, DEFAULT_QUEUE_SIZE);
        Actor<E> old = actors.putIfAbsent(actorPath, add);
        if (old == null) {
            LOG.info("create actorSystem: {}", actorPath);
            actorsCount.incrementAndGet();
            return add;
        }
        return old;
    }

    // 将actor入队的地方
    private <E> boolean schedule(Actor<E> actor, boolean hasMessageHint) {
        // 如果actor不能调度，则ret false
        if (!actor.canBeSchedule(hasMessageHint)) return false;
        // 设置actor为"可调度状态"
        if (actor.setAsScheduled()) {
            actor.submitTs = System.currentTimeMillis();
            this.executor.execute(actor);
            return true;
        }
        // actor.setAsScheduled()里，这里是actor已经是可调度状态，那么没必要再次入队
        return false;
    }

    // ActorSystem内定义的处理接口
    public interface Processor<T> {
        boolean process(T message, Actor<T> self);
    }

    public static class Actor<E> implements Runnable, Comparable<Actor> {
        // 初始状态
        private static final int Open = 0;
        // 可调度状态
        private static final int Scheduled = 2;
        // 掩码，二进制表示:11 与Open和Scheduled作&运算
        // shouldScheduleMask&currentStatus != Open 则为不可置为调度状态（当currentStatus为挂起状态或调度状态）
        private static final int shouldScheduleMask = 3;
        private static final int shouldNotProcessMask = ~2;
        // 挂起状态
        private static final int suspendUnit = 4;
        //每个actor至少执行的时间片
        private static final int QUOTA = 5;
        // status属性内存偏移量，用Unsafe操作
        private static long statusOffset;

        static {
            try {
                statusOffset = Unsafe.instance.objectFieldOffset(Actor.class.getDeclaredField("status"));
            } catch (Throwable t) {
                throw new ExceptionInInitializerError(t);
            }
        }

        final String systemName;
        final ActorSystem actorSystem;
        // actor内部维护的queue，后文简单分析下
        final BoundedNodeQueue<E> queue;
        // ActorSystem内部定义接口，PullMessageWorker实现的就是这个接口，用于真正业务逻辑处理的地方
        final Processor<E> processor;
        private final String name;
        // 一个actor执行总耗时
        private long total;
        // actor执行提交时间，即actor入队时间
        private volatile long submitTs;
        //通过Unsafe操作
        private volatile int status;

        Actor(String systemName, String name, ActorSystem actorSystem, Processor<E> processor, final int queueSize) {
            this.systemName = systemName;
            this.name = name;
            this.actorSystem = actorSystem;
            this.processor = processor;
            this.queue = new BoundedNodeQueue<>(queueSize);

            QMon.actorQueueGauge(systemName, name, () -> (double) queue.count());
        }

        // 入队，是actor内部的队列
        boolean dispatch(E message) {
            return queue.add(message);
        }

        // actor执行的地方
        @Override
        public void run() {
            long start = System.currentTimeMillis();
            String old = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(systemName + "-" + name);
                if (shouldProcessMessage()) {
                    processMessages();
                }
            } finally {
                long duration = System.currentTimeMillis() - start;
                // 每次actor执行的耗时累加到total
                total += duration;
                QMon.actorProcessTime(name, duration);

                Thread.currentThread().setName(old);
                // 设置为"空闲状态"，即初始状态 (currentStatus & ~Scheduled)
                setAsIdle();
                // 进行下一次调度
                this.actorSystem.schedule(this, false);
            }
        }

        void processMessages() {
            long deadline = System.currentTimeMillis() + QUOTA;
            while (true) {
                E message = queue.peek();
                if (message == null) return;
                // 处理业务逻辑
                boolean process = processor.process(message, this);
                // 失败，该message不会出队，等待下一次调度
                // 如pullMessageWorker中没有消息时将actor挂起
                if (!process) return;

                // 出队
                queue.pollNode();
                // 每个actor只有QUOTA个时间片的执行时间
                if (System.currentTimeMillis() >= deadline) return;
            }
        }

        final boolean shouldProcessMessage() {
            // 能够真正执行业务逻辑的判断
            // 一种场景是，针对挂起状态，由于没有拉取到消息该actor置为挂起状态
            // 自然就没有抢占时间片的必要了
            return (currentStatus() & shouldNotProcessMask) == 0;
        }

        // 能否调度
        private boolean canBeSchedule(boolean hasMessageHint) {
            int s = currentStatus();
            if (s == Open || s == Scheduled) return hasMessageHint || !queue.isEmpty();
            return false;
        }

        public final boolean resume() {
            while (true) {
                int s = currentStatus();
                int next = s < suspendUnit ? s : s - suspendUnit;
                if (updateStatus(s, next)) return next < suspendUnit;
            }
        }

        public final void suspend() {
            while (true) {
                int s = currentStatus();
                if (updateStatus(s, s + suspendUnit)) return;
            }
        }

        final boolean setAsScheduled() {
            while (true) {
                int s = currentStatus();
                // currentStatus为非Open状态，则ret false
                if ((s & shouldScheduleMask) != Open) return false;
                // 更新actor状态为调度状态
                if (updateStatus(s, s | Scheduled)) return true;
            }
        }

        final void setAsIdle() {
            while (true) {
                int s = currentStatus();
                // 更新actor状态位不可调度状态，(这里可以理解为更新为初始状态Open)
                if (updateStatus(s, s & ~Scheduled)) return;
            }
        }

        final int currentStatus() {
            // 根据status在内存中的偏移量取得status
            return Unsafe.instance.getIntVolatile(this, statusOffset);
        }

        private boolean updateStatus(int oldStatus, int newStatus) {
            // Unsafe 原子操作，处理status的轮转变更
            return Unsafe.instance.compareAndSwapInt(this, statusOffset, oldStatus, newStatus);
        }

        // 决定actor在优先级队列里的优先级的地方
        // 先看总耗时，以达到动态限速，保证执行"慢"的请求（已经堆积的消息拉取请求）在后执行
        // 其次看提交时间，先提交的actor先执行
        @Override
        public int compareTo(Actor o) {
            int result = Long.compare(total, o.total);
            return result == 0 ? Long.compare(submitTs, o.submitTs) : result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Actor<?> actor = (Actor<?>) o;
            return Objects.equals(systemName, actor.systemName) &&
                    Objects.equals(name, actor.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(systemName, name);
        }
    }

    /**
     * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
     */

    /**
     * Lock-free bounded non-blocking multiple-producer single-consumer queue based on the works of:
     * <p/>
     * Andriy Plokhotnuyk (https://github.com/plokhotnyuk)
     * - https://github.com/plokhotnyuk/actors/blob/2e65abb7ce4cbfcb1b29c98ee99303d6ced6b01f/src/test/scala/akka/dispatch/Mailboxes.scala
     * (Apache V2: https://github.com/plokhotnyuk/actors/blob/master/LICENSE)
     * <p/>
     * Dmitriy Vyukov's non-intrusive MPSC queue:
     * - http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
     * (Simplified BSD)
     *
     * 该类功能，是通过属性在内存的偏移量，结合cas原子操作来进行更新赋值等操作，以此来实现lock-free，这是比较常规的套路。值得一说的是Node里的setNext方法，这个方法的调用是在cas节点后，      对"上一位置"的next节点进行赋值。而这个方法使用的是Unsafe.instance.putOrderedObject，要说这个putOrderedObject，就不得不说MESI，缓存一致性协议。如volatile，当进行写操作时，它是依     靠storeload barrier来实现其他线程对此的可见性。而putOrderedObject也是依靠内存屏障，只不过是storestore barrier。storestore是比storeload快速的一种内存屏障。在硬件层面，内存屏障      分两种：Load-Barrier和Store-Barrier。Load-Barrier是让高速缓存中的数据失效，强制重新从主内存加载数据；Store-Barrier是让写入高速缓存的数据更新写入主内存，对其他线程可见。而java层     面的四种内存屏障无非是硬件层面的两种内存屏障的组合而已。那么可见，storestore barrier自然比storeload barrier快速。那么有一个问题，我们可不可以在这里也用cas操作呢？答案是可以，但      没必要。你可以想想这里为什么没必要。
     *
     */
    @SuppressWarnings("serial")
    private static class BoundedNodeQueue<T> {

        // 头结点、尾节点在内存中的偏移量
        private final static long enqOffset, deqOffset;

        static {
            try {
                enqOffset = Unsafe.instance.objectFieldOffset(BoundedNodeQueue.class.getDeclaredField("_enqDoNotCallMeDirectly"));
                deqOffset = Unsafe.instance.objectFieldOffset(BoundedNodeQueue.class.getDeclaredField("_deqDoNotCallMeDirectly"));
            } catch (Throwable t) {
                throw new ExceptionInInitializerError(t);
            }
        }

        private final int capacity;
        // 尾节点，通过enqOffset操作
        @SuppressWarnings("unused")
        private volatile Node<T> _enqDoNotCallMeDirectly;
        // 头结点，通过deqOffset操作
        @SuppressWarnings("unused")
        private volatile Node<T> _deqDoNotCallMeDirectly;

        protected BoundedNodeQueue(final int capacity) {
            if (capacity < 0) throw new IllegalArgumentException("AbstractBoundedNodeQueue.capacity must be >= 0");
            this.capacity = capacity;
            final Node<T> n = new Node<T>();
            setDeq(n);
            setEnq(n);
        }

        // 获取尾节点
        @SuppressWarnings("unchecked")
        private Node<T> getEnq() {
            // getObjectVolatile这种方式保证拿到的都是最新数据
            return (Node<T>) Unsafe.instance.getObjectVolatile(this, enqOffset);
        }

        // 设置尾节点，仅在初始化时用
        private void setEnq(Node<T> n) {
            Unsafe.instance.putObjectVolatile(this, enqOffset, n);
        }

        private boolean casEnq(Node<T> old, Node<T> nju) {
            // cas，循环设置，直到成功
            return Unsafe.instance.compareAndSwapObject(this, enqOffset, old, nju);
        }

        // 获取头结点
        @SuppressWarnings("unchecked")
        private Node<T> getDeq() {
            return (Node<T>) Unsafe.instance.getObjectVolatile(this, deqOffset);
        }

        // 仅在初始化时用
        private void setDeq(Node<T> n) {
            Unsafe.instance.putObjectVolatile(this, deqOffset, n);
        }

        // cas设置头结点
        private boolean casDeq(Node<T> old, Node<T> nju) {
            return Unsafe.instance.compareAndSwapObject(this, deqOffset, old, nju);
        }

        // 与其叫count，不如唤作index，但是是否应该考虑溢出的情况？
        public final int count() {
            final Node<T> lastNode = getEnq();
            final int lastNodeCount = lastNode.count;
            return lastNodeCount - getDeq().count;
        }

        /**
         * @return the maximum capacity of this queue
         */
        public final int capacity() {
            return capacity;
        }

        // Possible TODO — impl. could be switched to addNode(new Node(value)) if we want to allocate even if full already
        public final boolean add(final T value) {
            for (Node<T> n = null; ; ) {
                final Node<T> lastNode = getEnq();
                final int lastNodeCount = lastNode.count;
                if (lastNodeCount - getDeq().count < capacity) {
                    // Trade a branch for avoiding to create a new node if full,
                    // and to avoid creating multiple nodes on write conflict á la Be Kind to Your GC
                    if (n == null) {
                        n = new Node<T>();
                        n.value = value;
                    }

                    n.count = lastNodeCount + 1; // Piggyback on the HB-edge between getEnq() and casEnq()

                    // Try to putPullLogs the node to the end, if we fail we continue loopin'
                    // 相当于
                    // enq -> next = new Node(value); enq = neq -> next
                    if (casEnq(lastNode, n)) {
                        lastNode.setNext(n);
                        return true;
                    }
                } else return false; // Over capacity—couldn't add the node
            }
        }

        public final boolean isEmpty() {
            // enq == deq 即为empty
            return getEnq() == getDeq();
        }

        /**
         * Removes the first element of this queue if any
         *
         * @return the value of the first element of the queue, null if empty
         */
        public final T poll() {
            final Node<T> n = pollNode();
            return (n != null) ? n.value : null;
        }

        public final T peek() {
            Node<T> n = peekNode();
            return (n != null) ? n.value : null;
        }

        @SuppressWarnings("unchecked")
        protected final Node<T> peekNode() {
            for (; ; ) {
                final Node<T> deq = getDeq();
                final Node<T> next = deq.next();
                if (next != null || getEnq() == deq)
                    return next;
            }
        }

        /**
         * Removes the first element of this queue if any
         *
         * @return the `Node` of the first element of the queue, null if empty
         */
        public final Node<T> pollNode() {
            for (; ; ) {
                final Node<T> deq = getDeq();
                final Node<T> next = deq.next();
                if (next != null) {
                    if (casDeq(deq, next)) {
                        deq.value = next.value;
                        deq.setNext(null);
                        next.value = null;
                        return deq;
                    } // else we retry (concurrent consumers)
                    // 比较套路的cas操作，就不多说了
                } else if (getEnq() == deq) return null; // If we got a null and head meets tail, we are empty
            }
        }

        public static class Node<T> {
            private final static long nextOffset;

            static {
                try {
                    nextOffset = Unsafe.instance.objectFieldOffset(Node.class.getDeclaredField("_nextDoNotCallMeDirectly"));
                } catch (Throwable t) {
                    throw new ExceptionInInitializerError(t);
                }
            }

            protected T value;
            protected int count;
            // 也是利用偏移量操作
            @SuppressWarnings("unused")
            private volatile Node<T> _nextDoNotCallMeDirectly;

            @SuppressWarnings("unchecked")
            public final Node<T> next() {
                return (Node<T>) Unsafe.instance.getObjectVolatile(this, nextOffset);
            }

            protected final void setNext(final Node<T> newNext) {
                // 这里有点讲究，下面分析下
                Unsafe.instance.putOrderedObject(this, nextOffset, newNext);
            }
        }
    }

    static class Unsafe {
        public final static sun.misc.Unsafe instance;

        static {
            try {
                sun.misc.Unsafe found = null;
                for (Field field : sun.misc.Unsafe.class.getDeclaredFields()) {
                    if (field.getType() == sun.misc.Unsafe.class) {
                        field.setAccessible(true);
                        found = (sun.misc.Unsafe) field.get(null);
                        break;
                    }
                }
                if (found == null) throw new IllegalStateException("Can't find instance of sun.misc.Unsafe");
                else instance = found;
            } catch (Throwable t) {
                throw new ExceptionInInitializerError(t);
            }
        }
    }
}
