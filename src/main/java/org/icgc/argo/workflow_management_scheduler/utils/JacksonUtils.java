package org.icgc.argo.workflow_management_scheduler.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc.argo.workflow_management_scheduler.model.JsonField;

public class JacksonUtils {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @SneakyThrows
  public static JsonNode readTree(String jsonStr) {
    return MAPPER.readTree(jsonStr);
  }

  public static JsonNode putInJsonNode(JsonNode jsonNode, List<JsonField> fieldsToUpdate) {
    val mutableJsonNode = jsonNode.deepCopy();

    fieldsToUpdate.forEach(
        field -> {
          val fieldPath = field.getPath();
          val fieldValue = field.getValue();

          val indexOfSplit = fieldPath.lastIndexOf(".");

          String fieldName;
          ObjectNode node;
          if (indexOfSplit == -1) {
            node = (ObjectNode) mutableJsonNode;
            fieldName = fieldPath;
          } else {
            val fieldParentPath = fieldPath.substring(0, indexOfSplit);
            val nodePointer = "/" + fieldParentPath.replaceAll("\\.", "/");
            node = (ObjectNode) mutableJsonNode.at(nodePointer);

            fieldName = fieldPath.substring(indexOfSplit + 1);
          }

          if (fieldValue instanceof Integer) {
            node.put(fieldName, (Integer) fieldValue);
          } else if (fieldValue instanceof Boolean) {
            node.put(fieldName, (Boolean) fieldValue);
          } else {
            node.put(fieldName, fieldValue.toString());
          }
        });
    return mutableJsonNode;
  }
}
