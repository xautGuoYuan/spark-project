package com.ibeifeng.sparkproject.test;

/**
 * 单例模式Demo
 * 
 * 私有化构造方法
 * 含有静态方法来提供唯一实例的功能：getInstance(),该方法必须保证类的实例创建，且仅创建一次，返回
 * 一个唯一的实例
 * 
 * 应用场景：
 * 1，可以在读取大量的配置信息之后，用单利模式的方式，就将配置信息仅仅保存在一个实例的实例
 * 变量中，这样可以比喵对于静态不变的配置信息，反复多次的读取。
 * 
 * 2，JDBC辅助类，全局就只有一个实例，实例中持有一个内部的简单数据源，使用了单例模式之后，就
 * 保证只有一个实例，那么数据源也只有一个，不会重复创建多次数据源（数据库连接池）
 * 
 * @author Administrator
 *
 */
public class Singleton {

	//首先必须有一个私有的静态变量，来引用自己即将被创建出来的单例
	private static Singleton instance = null;
	
	private Singleton() {
		
	}
	
	/**
	 * 创建唯一实例并返回
	 * 
	 * 必须考虑多线程并发访问问题：可能有多个线程同时过来获取单例，那么可能导致多次创建单例
	 * 所以，这个方法，通常需要进行多线程并发访问安全的控制
	 * 
	 * 如果采用以下方式：
	 * public static synchronized Singleton getInstance()方法
	 * 但是这样做有一个很大的问题，在第一次调用的时候，的确可以做到避免多个线程并发访问创建
	 * 多个实例的问题。第一次访问的时候，走if里面的语句进行判断，但是以后，多个线程进行访问的
	 * 时候，他们仅仅只需要读取这个单例就行了，并不需要锁，如果在这个方法上面加上同步，那么将
	 * 导致后面的效率跟不上
	 * @return
	 */
	public static Singleton getInstance() {
		//两步检查机制
		//首先第一步，多个线程过来的时候，判断instance是否为null
		//如果为null再往下走
		if(instance == null) {
			//在，这里，进行多个线程的同步
			//同一时间，只能有一个线程获取到Singleton Class对象的锁
			//进入后续的代码
			//其他线程，都是只能够在原地等待，获取锁
			synchronized (Singleton.class) {
				//只要第一个线程获取到锁的时候，进入到这里， 才会发现instance是null
				//此时才会去创建这个单例
				//此后，线程，哪怕是走到了这一步，也会发现instance已经不是null了
				//就不会反复创建一个单例
				if(instance == null) {
					instance = new Singleton();
				}
			}
		}
		return instance;
	}
}
