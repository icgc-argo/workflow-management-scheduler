package org.icgc.argo.workflow_management_scheduler;

import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class WfMgmtMiddlewareApplicationTests {

  // Context load test disabled because this service uses rabbitmq and will fail to load without it
  //  @Test
  //  void contextLoads() {}
}
