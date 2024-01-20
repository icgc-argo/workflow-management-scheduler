package org.icgc.argo.workflow_management_scheduler.utils;

import lombok.experimental.UtilityClass;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@UtilityClass
public class DirectoryUtils {

  private final String DIRECTORY_CLUSTER_DELIMITER = ":";
  private final String DIRECTORY_CLUSTER_PATTERN = ".*:.*";

  public String getDirectory(String dirValue){
    return dirValue.split(DIRECTORY_CLUSTER_DELIMITER)[0];
  }

  public String getCluster(String dirValue){
    return dirValue.split(DIRECTORY_CLUSTER_DELIMITER)[1];
  }

  public static void checkDirectoryClusterPattern(String strToCheck, String message){
    Matcher matcher = Pattern.compile(DIRECTORY_CLUSTER_PATTERN).matcher(strToCheck);
    if(!matcher.matches()){
      throw new RuntimeException(message);
    }
  }

}
