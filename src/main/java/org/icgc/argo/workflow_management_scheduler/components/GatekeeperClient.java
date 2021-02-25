package org.icgc.argo.workflow_management_scheduler.components;

import static org.icgc.argo.workflow_management_scheduler.model.GqlResult.EMPTY_SEARCH_RESULT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.val;
import org.icgc.argo.workflow_management_scheduler.model.GqlResult;
import org.icgc.argo.workflow_management_scheduler.model.Run;
import org.icgc.argo.workflow_management_scheduler.model.SimpleQuery;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Component
public class GatekeeperClient {
  private static final Integer DEFAULT_PAGE_SIZE = 100;

  @Value("${gatekeeper.url}")
  private String url;

  public Mono<List<Run>> getAllRuns() {
    return getAllRuns(0)
        // Expand here is basically being used to recursively get the next page of runs from
        // gatekeeper. It returns a Flux of the Object in each sub Mono.
        .expand(
            searchResultTuple2 -> {
              val currentPageNum = searchResultTuple2.getT1();
              val searchResult = searchResultTuple2.getT2();
              if (searchResult.getInfo().getHasNextFrom()) {
                return getAllRuns(currentPageNum + 1);
              } else {
                return Mono.empty();
              }
            })
        .map(res -> res.getT2().getContent())
        .reduce(
            new ArrayList<>(),
            (acc, curr) -> {
              acc.addAll(curr);
              return acc;
            });
  }

  private Mono<Tuple2<Integer, GqlResult.ActiveRunsSearchResult>> getAllRuns(Integer page) {
    return WebClient.create()
        .post()
        .uri(url)
        .contentType(MediaType.APPLICATION_JSON)
        .body(BodyInserters.fromValue(new SimpleQuery(createQuery(page), Map.of())))
        .retrieve()
        .bodyToMono(GqlResult.class)
        .map(
            gqlResult -> {
              val searchResult =
                  gqlResult != null
                          && gqlResult.getData() != null
                          && gqlResult.getData().getActiveRuns() != null
                      ? gqlResult.getData().getActiveRuns()
                      : EMPTY_SEARCH_RESULT;

              return Tuples.of(page, searchResult);
            });
  }

  private static String createQuery(Integer from) {
    return "query  {\n"
        + "  activeRuns (page: {from: "
        + from
        + ", size: "
        + DEFAULT_PAGE_SIZE
        + "}) {\n"
        + "    content {\n"
        + "\t\t\trunId\n"
        + "      state\n"
        + "      workflowUrl\n"
        + "      workflowType\n"
        + "      workflowTypeVersion\n"
        + "      workflowParamsJsonStr\n"
        + "      timestamp\n"
        + "      workflowEngineParams {\n"
        + "        resume\n"
        + "        revision\n"
        + "        projectDir\n"
        + "        launchDir\n"
        + "        defaultContainer\n"
        + "        latest\n"
        + "      }\n"
        + "    }    \n"
        + "  info {\n"
        + "    contentCount\n"
        + "    hasNextFrom\n"
        + "    totalHits\n"
        + "  }\n"
        + "  }\n"
        + "}\n";
  }
}
