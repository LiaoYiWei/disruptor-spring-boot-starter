package com.lmax.disruptor.spring.boot;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.spring.boot.annotation.EventRule;
import com.lmax.disruptor.spring.boot.config.EventHandlerDefinition;
import com.lmax.disruptor.spring.boot.config.Ini;
import com.lmax.disruptor.spring.boot.context.DisruptorEventAwareProcessor;
import com.lmax.disruptor.spring.boot.event.DisruptorApplicationEvent;
import com.lmax.disruptor.spring.boot.event.DisruptorEvent;
import com.lmax.disruptor.spring.boot.event.factory.DisruptorBindEventFactory;
import com.lmax.disruptor.spring.boot.event.factory.DisruptorEventThreadFactory;
import com.lmax.disruptor.spring.boot.event.handler.DisruptorEventDispatcher;
import com.lmax.disruptor.spring.boot.event.handler.DisruptorHandler;
import com.lmax.disruptor.spring.boot.event.handler.chain.HandlerChainManager;
import com.lmax.disruptor.spring.boot.event.handler.chain.def.PathMatchingHandlerChainResolver;
import com.lmax.disruptor.spring.boot.event.translator.DisruptorEventOneArgTranslator;
import com.lmax.disruptor.spring.boot.event.translator.DisruptorEventThreeArgTranslator;
import com.lmax.disruptor.spring.boot.event.translator.DisruptorEventTwoArgTranslator;
import com.lmax.disruptor.spring.boot.util.DisruptorUtils;
import com.lmax.disruptor.spring.boot.util.StringUtils;
import com.lmax.disruptor.spring.boot.util.WaitStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.OrderComparator;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ThreadFactory;

@Configuration
@ConditionalOnClass({ Disruptor.class })
@ConditionalOnProperty(prefix = DisruptorProperties.PREFIX, value = "enabled", havingValue = "true")
@EnableConfigurationProperties({ DisruptorProperties.class })
@SuppressWarnings({ "unchecked", "rawtypes" })
public class DisruptorAutoConfiguration implements ApplicationContextAware {

	private static final Logger LOG = LoggerFactory.getLogger(DisruptorAutoConfiguration.class);
	private ApplicationContext applicationContext;
	/**
	 * 处理器链定义
	 */
	private Map<String, String> handlerChainDefinitionMap = new HashMap<String, String>();
	
	/**
	 * 决定一个消费者将如何等待生产者将Event置入Disruptor的策略。用来权衡当生产者无法将新的事件放进RingBuffer时的处理策略。
	 * （例如：当生产者太快，消费者太慢，会导致生成者获取不到新的事件槽来插入新事件，则会根据该策略进行处理，默认会堵塞）
	 * @return {@link WaitStrategy} instance
	 */
	@Bean
	@ConditionalOnMissingBean
	public WaitStrategy waitStrategy() {
		return WaitStrategies.YIELDING_WAIT;
	}

	@Bean
	@ConditionalOnMissingBean
	public ThreadFactory threadFactory() {
		return new DisruptorEventThreadFactory();
	}

	@Bean
	@ConditionalOnMissingBean
	public EventFactory<DisruptorEvent> eventFactory() {
		return new DisruptorBindEventFactory();
	}
	
	/*
	 * Handler实现集合
	 */
	@Bean("disruptorHandlers")
	public Map<String, DisruptorHandler<DisruptorEvent>> disruptorHandlers() {

		Map<String, DisruptorHandler<DisruptorEvent>> disruptorPreHandlers = new LinkedHashMap<String, DisruptorHandler<DisruptorEvent>>();

		Map<String, DisruptorHandler> beansOfType = getApplicationContext().getBeansOfType(DisruptorHandler.class);
		if (!ObjectUtils.isEmpty(beansOfType)) {
			Iterator<Entry<String, DisruptorHandler>> ite = beansOfType.entrySet().iterator();
			while (ite.hasNext()) {
				Entry<String, DisruptorHandler> entry = ite.next();
				if (entry.getValue() instanceof DisruptorEventDispatcher) {
					// 跳过入口实现类
					continue;
				}
				
				EventRule annotationType = getApplicationContext().findAnnotationOnBean(entry.getKey(), EventRule.class);

				if(annotationType == null) {
					// 注解为空，则打印错误信息
					LOG.error("Not Found AnnotationType {0} on Bean {1} Whith Name {2}", EventRule.class, entry.getValue().getClass(), entry.getKey());
				} else {
					handlerChainDefinitionMap.put(annotationType.value(), entry.getKey());
				}
				
				disruptorPreHandlers.put(entry.getKey(), entry.getValue());
			}
		}
		// BeanFactoryUtils.beansOfTypeIncludingAncestors(getApplicationContext(),
		// EventHandler.class);

		return disruptorPreHandlers;
	}

	/*
	 * 处理器链集合
	 */
	@Bean("disruptorEventHandlers")
	public List<DisruptorEventDispatcher> disruptorEventHandlers(DisruptorProperties properties,
			@Qualifier("disruptorHandlers") Map<String, DisruptorHandler<DisruptorEvent>> eventHandlers) {
		// 获取定义 拦截链规则
		List<EventHandlerDefinition> handlerDefinitions = properties.getHandlerDefinitions();
		// 拦截器集合
		List<DisruptorEventDispatcher> disruptorEventHandlers = new ArrayList<DisruptorEventDispatcher>();
		// 未定义，则使用默认规则
		if (CollectionUtils.isEmpty(handlerDefinitions)) {
			
			EventHandlerDefinition definition = new EventHandlerDefinition();
			
			definition.setOrder(0);
			definition.setDefinitionMap(handlerChainDefinitionMap);
			
			// 构造DisruptorEventHandler
			disruptorEventHandlers.add(this.createDisruptorEventHandler(definition, eventHandlers));
			
		} else {
			// 迭代拦截器规则
			for (EventHandlerDefinition handlerDefinition : handlerDefinitions) {
	
				// 构造DisruptorEventHandler
				disruptorEventHandlers.add(this.createDisruptorEventHandler(handlerDefinition, eventHandlers));
	
			}
		}
		// 进行排序
		Collections.sort(disruptorEventHandlers, new OrderComparator());

		return disruptorEventHandlers;
	}

	/*
	 * 构造DisruptorEventHandler
	 */
	protected DisruptorEventDispatcher createDisruptorEventHandler(EventHandlerDefinition handlerDefinition,
			Map<String, DisruptorHandler<DisruptorEvent>> eventHandlers) {

		if (StringUtils.isNotEmpty(handlerDefinition.getDefinitions())) {
			handlerChainDefinitionMap.putAll(this.parseHandlerChainDefinitions(handlerDefinition.getDefinitions()));
		} else if (!CollectionUtils.isEmpty(handlerDefinition.getDefinitionMap())) {
			handlerChainDefinitionMap.putAll(handlerDefinition.getDefinitionMap());
		}

		HandlerChainManager<DisruptorEvent> manager = DisruptorUtils.createHandlerChainManager(eventHandlers, handlerChainDefinitionMap);
		PathMatchingHandlerChainResolver chainResolver = new PathMatchingHandlerChainResolver();
		chainResolver.setHandlerChainManager(manager);
		return new DisruptorEventDispatcher(chainResolver, handlerDefinition.getOrder());
	}

	protected Map<String, String> parseHandlerChainDefinitions(String definitions) {
		Ini ini = new Ini();
		ini.load(definitions);
		Ini.Section section = ini.getSection("urls");
		if (CollectionUtils.isEmpty(section)) {
			section = ini.getSection(Ini.DEFAULT_SECTION_NAME);
		}
		return section;
	}



	/**
	 * 
	 * <p>
	 * 创建Disruptor
	 * </p>
	 * <p>
	 * 1 eventFactory 为
	 * <p>
	 * 2 ringBufferSize为RingBuffer缓冲区大小，最好是2的指数倍
	 * </p>
	 * 
	 * @param properties	: 配置参数
	 * @param waitStrategy 	: 一种策略，用来均衡数据生产者和消费者之间的处理效率，默认提供了3个实现类
	 * @param threadFactory	: 线程工厂
	 * @param eventFactory	: 工厂类对象，用于创建一个个的LongEvent， LongEvent是实际的消费数据，初始化启动Disruptor的时候，Disruptor会调用该工厂方法创建一个个的消费数据实例存放到RingBuffer缓冲区里面去，创建的对象个数为ringBufferSize指定的
	 * @param disruptorEventHandlers	: 事件分发器
	 * @return {@link Disruptor} instance
	 */
	@Bean
	@ConditionalOnClass({ Disruptor.class })
	@ConditionalOnProperty(prefix = DisruptorProperties.PREFIX, value = "enabled", havingValue = "true")
	public Disruptor<DisruptorEvent> disruptor(
			DisruptorProperties properties, 
			WaitStrategy waitStrategy,
			ThreadFactory threadFactory, 
			EventFactory<DisruptorEvent> eventFactory,
			@Qualifier("disruptorEventHandlers") 
			List<DisruptorEventDispatcher> disruptorEventHandlers) {

		// http://blog.csdn.net/a314368439/article/details/72642653?utm_source=itdadao&utm_medium=referral
		if (properties.isMultiProducer()) {
			return DisruptorUtils.createDisruptor(threadFactory, eventFactory, disruptorEventHandlers, properties.getRingBufferSize(), ProducerType.MULTI, waitStrategy);
		}
		return DisruptorUtils.createDisruptor(threadFactory, eventFactory, disruptorEventHandlers, properties.getRingBufferSize(), ProducerType.SINGLE, waitStrategy);

	}
	
	@Bean
	@ConditionalOnMissingBean
	public EventTranslatorOneArg<DisruptorEvent, DisruptorEvent> oneArgEventTranslator() {
		return new DisruptorEventOneArgTranslator();
	}
	
	@Bean
	@ConditionalOnMissingBean
	public EventTranslatorTwoArg<DisruptorEvent, String, String> twoArgEventTranslator() {
		return new DisruptorEventTwoArgTranslator();
	}
	
	@Bean
	@ConditionalOnMissingBean
	public EventTranslatorThreeArg<DisruptorEvent, String, String, String> threeArgEventTranslator() {
		return new DisruptorEventThreeArgTranslator();
	}

	@Bean
	public DisruptorTemplate disruptorTemplate(Disruptor disruptor, EventTranslatorOneArg<DisruptorEvent, DisruptorEvent> oneArgEventTranslator) {
		return new DisruptorTemplate(disruptor, oneArgEventTranslator);
	}
	
	@Bean
	public ApplicationListener<DisruptorApplicationEvent> disruptorEventListener(Disruptor<DisruptorEvent> disruptor,
			EventTranslatorOneArg<DisruptorEvent, DisruptorEvent> oneArgEventTranslator) {
		return new ApplicationListener<DisruptorApplicationEvent>() {

			@Override
			public void onApplicationEvent(DisruptorApplicationEvent appEvent) {
				DisruptorEvent event = (DisruptorEvent) appEvent.getSource();
				disruptor.publishEvent(oneArgEventTranslator, event);
			}
			
		};
	}
	
	@Bean
	public DisruptorEventAwareProcessor disruptorEventAwareProcessor() {
		return new DisruptorEventAwareProcessor();
	}
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

}
