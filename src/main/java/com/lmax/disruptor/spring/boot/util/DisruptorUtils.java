package com.lmax.disruptor.spring.boot.util;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.spring.boot.annotation.EventRule;
import com.lmax.disruptor.spring.boot.config.EventHandlerDefinition;
import com.lmax.disruptor.spring.boot.event.DisruptorEvent;
import com.lmax.disruptor.spring.boot.event.factory.DisruptorBindEventFactory;
import com.lmax.disruptor.spring.boot.event.factory.DisruptorEventThreadFactory;
import com.lmax.disruptor.spring.boot.event.handler.DisruptorEventDispatcher;
import com.lmax.disruptor.spring.boot.event.handler.DisruptorHandler;
import com.lmax.disruptor.spring.boot.event.handler.Nameable;
import com.lmax.disruptor.spring.boot.event.handler.chain.HandlerChainManager;
import com.lmax.disruptor.spring.boot.event.handler.chain.def.DefaultHandlerChainManager;
import com.lmax.disruptor.spring.boot.event.handler.chain.def.PathMatchingHandlerChainResolver;
import com.lmax.disruptor.spring.boot.hooks.DisruptorShutdownHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.core.OrderComparator;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.util.*;
import java.util.concurrent.ThreadFactory;

/**
 * <p>注释</p>
 *
 * @author liaoyiwei
 */
public class DisruptorUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DisruptorUtils.class);

    public static Disruptor<DisruptorEvent> createDefaultDisruptor(List<DisruptorEventDispatcher> disruptorEventHandlers,
                                                                   int ringBufferSize) {
        return createDisruptor(new DisruptorEventThreadFactory(), new DisruptorBindEventFactory(), disruptorEventHandlers, ringBufferSize, ProducerType.SINGLE, WaitStrategies.YIELDING_WAIT);
    }

    public static Disruptor<DisruptorEvent> createDisruptor(ThreadFactory threadFactory, EventFactory<DisruptorEvent> eventFactory, List<DisruptorEventDispatcher> disruptorEventHandlers, int ringBufferSize, ProducerType producerType, WaitStrategy waitStrategy) {
        Disruptor<DisruptorEvent> disruptor = new Disruptor<>(eventFactory, ringBufferSize, threadFactory,
                producerType, waitStrategy);
        if (!ObjectUtils.isEmpty(disruptorEventHandlers)) {
            // 进行排序
            Collections.sort(disruptorEventHandlers, new OrderComparator());
            // 使用disruptor创建消费者组
            EventHandlerGroup<DisruptorEvent> handlerGroup = null;
            for (int i = 0; i < disruptorEventHandlers.size(); i++) {
                // 连接消费事件方法，其中EventHandler的是为消费者消费消息的实现类
                DisruptorEventDispatcher eventHandler = disruptorEventHandlers.get(i);
                if (i < 1) {
                    handlerGroup = disruptor.handleEventsWith(eventHandler);
                } else {
                    // 完成前置事件处理之后执行后置事件处理
                    handlerGroup.then(eventHandler);
                }
            }
        }
        // 启动
        disruptor.start();
        /**
         * 应用退出时，要调用shutdown来清理资源，关闭网络连接，从MetaQ服务器上注销自己
         * 注意：我们建议应用在JBOSS、Tomcat等容器的退出钩子里调用shutdown方法
         */
        Runtime.getRuntime().addShutdownHook(new DisruptorShutdownHook(disruptor));
        return disruptor;
    }

    public static DisruptorEventDispatcher createDisruptorEventHandler(ApplicationContext context, String disruptorName) {
        Map<String, String> handlerChainDefinitionMap = new HashMap<>();
        Map<String, DisruptorHandler<DisruptorEvent>> disruptorPreHandlers = new LinkedHashMap<String, DisruptorHandler<DisruptorEvent>>();
        Map<String, DisruptorHandler> beansOfType = context.getBeansOfType(DisruptorHandler.class);
        if (!ObjectUtils.isEmpty(beansOfType)) {
            Iterator<Map.Entry<String, DisruptorHandler>> ite = beansOfType.entrySet().iterator();
            while (ite.hasNext()) {
                Map.Entry<String, DisruptorHandler> entry = ite.next();
                if (entry.getValue() instanceof DisruptorEventDispatcher) {
                    // 跳过入口实现类
                    continue;
                }
                EventRule annotationType = context.findAnnotationOnBean(entry.getKey(), EventRule.class);
                if (annotationType == null) {
                    // 注解为空，则打印错误信息
                    LOG.error("Not Found AnnotationType {0} on Bean {1} Whith Name {2}", EventRule.class, entry.getValue().getClass(), entry.getKey());
                } else if (annotationType.disruptorName().equals(disruptorName)) {
                    handlerChainDefinitionMap.put(annotationType.value(), entry.getKey());
                    disruptorPreHandlers.put(entry.getKey(), entry.getValue());
                }
            }
        }
        HandlerChainManager<DisruptorEvent> manager = createHandlerChainManager(disruptorPreHandlers, handlerChainDefinitionMap);
        PathMatchingHandlerChainResolver chainResolver = new PathMatchingHandlerChainResolver();
        chainResolver.setHandlerChainManager(manager);
        return new DisruptorEventDispatcher(chainResolver, new EventHandlerDefinition().getOrder());
    }

    public static HandlerChainManager<DisruptorEvent> createHandlerChainManager(
            Map<String, DisruptorHandler<DisruptorEvent>> eventHandlers,
            Map<String, String> handlerChainDefinitionMap) {
        HandlerChainManager<DisruptorEvent> manager = new DefaultHandlerChainManager();
        if (!CollectionUtils.isEmpty(eventHandlers)) {
            for (Map.Entry<String, DisruptorHandler<DisruptorEvent>> entry : eventHandlers.entrySet()) {
                String name = entry.getKey();
                DisruptorHandler<DisruptorEvent> handler = entry.getValue();
                if (handler instanceof Nameable) {
                    ((Nameable) handler).setName(name);
                }
                manager.addHandler(name, handler);
            }
        }

        if (!CollectionUtils.isEmpty(handlerChainDefinitionMap)) {
            for (Map.Entry<String, String> entry : handlerChainDefinitionMap.entrySet()) {
                // ant匹配规则
                String rule = entry.getKey();
                String chainDefinition = entry.getValue();
                manager.createChain(rule, chainDefinition);
            }
        }

        return manager;
    }


}
