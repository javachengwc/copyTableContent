package org.saiku.copyTableContent;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "" })
public class SpringTest{

	@Test
	public void testConn() {
		System.out.println("hello");
	}
}
