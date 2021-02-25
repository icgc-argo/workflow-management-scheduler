package org.icgc.argo.workflow_management_scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.argo.workflow_management_scheduler.utils.JacksonUtils.putInJsonNode;
import static org.icgc.argo.workflow_management_scheduler.utils.JacksonUtils.readTree;

import java.util.List;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc.argo.workflow_management_scheduler.model.JsonField;
import org.junit.jupiter.api.Test;

public class JacksonUtilsTests {
  @Test
  @SneakyThrows
  void testPutInJsonNode() {
    val jsonStr =
        "{"
            + "\"fieldA\": \"asdf\","
            + "\"fieldB\": {"
            + "\"fieldC\": \"fdsa\","
            + "\"fieldD\": {"
            + "\"fieldE\": \"qwerty\""
            + "}"
            + "}"
            + "}";

    val jsonNode = readTree(jsonStr);
    val NEW_VALUE = 1;
    val fieldsToUpdate =
        List.of(new JsonField("fieldB.fieldD.cpus", NEW_VALUE), new JsonField("cpus", NEW_VALUE));

    val resultJsonNode = putInJsonNode(jsonNode, fieldsToUpdate);
    assertThat(resultJsonNode.at("/fieldB/fieldD/cpus").asInt()).isEqualTo(NEW_VALUE);
    assertThat(resultJsonNode.at("/cpus").asInt()).isEqualTo(NEW_VALUE);
  }
}
