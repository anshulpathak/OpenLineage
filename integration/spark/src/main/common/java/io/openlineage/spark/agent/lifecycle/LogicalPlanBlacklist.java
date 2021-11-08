package io.openlineage.spark.agent.lifecycle;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;

/**
 * Some providers, like Delta Lake do things like running queries on metadata to figure out what
 * files should be read for a particular version of a Delta table. To not emit events that do not
 * correspond to actions done on data (not metadata) we need to filter those out.
 *
 * <p>TODO: databricks does the same with some particular actions like clicking on DBFS browser on
 * the databricks web console. We need to filter those too.
 */
public class LogicalPlanBlacklist {

  static List<String> blacklistedOutputClasses =
      Stream.of(Aggregate.class, LogicalRelation.class, LocalRelation.class, GlobalLimit.class)
          .map(Class::getCanonicalName)
          .collect(Collectors.toList());

  static boolean isBlacklistedOutput(LogicalPlan x) {
    return blacklistedOutputClasses.contains(x.getClass().getCanonicalName());
  }
}
