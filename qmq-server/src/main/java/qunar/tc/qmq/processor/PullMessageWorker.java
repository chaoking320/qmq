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

package qunar.tc.qmq.processor;

import qunar.tc.qmq.base.PullMessageResult;
import qunar.tc.qmq.concurrent.ActorSystem;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.store.MessageStoreWrapper;
import qunar.tc.qmq.utils.ConsumerGroupUtils;
import qunar.tc.qmq.utils.ObjectUtils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yunfeng.yang
 * @since 2017/10/30
 */
class PullMessageWorker implements ActorSystem.Processor<PullMessageProcessor.PullEntry> {

    private static final Object HOLDER = new Object();

    private final MessageStoreWrapper store;
    private final ActorSystem actorSystem;
    private final ConcurrentMap<String, ConcurrentMap<String, Object>> subscribers;

    PullMessageWorker(MessageStoreWrapper store, ActorSystem actorSystem) {
        this.store = store;
        this.actorSystem = actorSystem;
        this.subscribers = new ConcurrentHashMap<>();
    }

    void pull(PullMessageProcessor.PullEntry pullEntry) {
        // subject+group作actor调度粒度
        final String actorPath = ConsumerGroupUtils.buildConsumerGroupKey(pullEntry.subject, pullEntry.group);
        // actor调度
        actorSystem.dispatch(actorPath, pullEntry, this);
    }

    @Override
    public boolean process(PullMessageProcessor.PullEntry entry, ActorSystem.Actor<PullMessageProcessor.PullEntry> self) {
        QMon.pullQueueTime(entry.subject, entry.group, entry.pullBegin);

        //开始处理请求的时候就过期了，那么就直接不处理了，也不返回任何东西给客户端，客户端等待超时
        //因为出现这种情况一般是server端排队严重，暂时挂起客户端可以避免情况恶化
        // deadline机制，如果QMQ认为这个消费请求来不及处理，那么就直接返回，避免雪崩
        if (entry.expired()) {
            QMon.pullExpiredCountInc(entry.subject, entry.group);
            return true;
        }

        if (entry.isInValid()) {
            QMon.pullInValidCountInc(entry.subject, entry.group);
            return true;
        }

        // 存储层find消息
        final PullMessageResult pullMessageResult = store.findMessages(entry.pullRequest);

        if (pullMessageResult == PullMessageResult.FILTER_EMPTY ||
                pullMessageResult.getMessageNum() > 0
                || entry.isPullOnce()
                || entry.isTimeout()) {
            entry.processMessageResult(pullMessageResult);
            return true;
        }

        // 没有拉取到消息，那么挂起该actor
        self.suspend();
        if (entry.setTimerOnDemand()) {
            QMon.suspendRequestCountInc(entry.subject, entry.group);
            // 订阅消息，一有消息来就唤醒该actor
            subscribe(entry.subject, entry.group);
            return false;
        }

        // 已经超时，那么即刻唤醒调度
        self.resume();
        entry.processNoMessageResult();
        return true;
    }

    // 订阅
    private void subscribe(String subject, String group) {
        ConcurrentMap<String, Object> map = subscribers.get(subject);
        if (map == null) {
            map = new ConcurrentHashMap<>();
            map = ObjectUtils.defaultIfNull(subscribers.putIfAbsent(subject, map), map);
        }
        map.putIfAbsent(group, HOLDER);
    }

    // 有消息来就唤醒订阅的subscriber
    void remindNewMessages(final String subject) {
        final ConcurrentMap<String, Object> map = this.subscribers.get(subject);
        if (map == null) return;

        for (String group : map.keySet()) {
            map.remove(group);
            this.actorSystem.resume(ConsumerGroupUtils.buildConsumerGroupKey(subject, group));
            QMon.resumeActorCountInc(subject, group);
        }
    }
}
