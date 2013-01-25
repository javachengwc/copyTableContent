package ylutil;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SpringHelper {
	private static ApplicationContext ac = null;
	private SpringHelper(){
		
	}
	private static Object lock = new Object();
	public static ApplicationContext getInstance(){
		if(ac == null){
			synchronized(lock){
				if(ac == null){
					return new ClassPathXmlApplicationContext("applicationContext.xml");
				}
			}
		}
		return ac;
	}
	
	/**
	 * use E
	 * @param beanName
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <E>E getBean(String beanName){
		return (E)ac.getBean(beanName);
	}
	
}
