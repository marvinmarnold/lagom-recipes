package com.lightbend.lagom.recipes.mixedpersistence.hello.impl.readside;

import com.lightbend.lagom.javadsl.persistence.AggregateEventTag;
import com.lightbend.lagom.javadsl.persistence.ReadSide;
import com.lightbend.lagom.javadsl.persistence.ReadSideProcessor;
import com.lightbend.lagom.javadsl.persistence.jdbc.JdbcSession;
import com.lightbend.lagom.recipes.mixedpersistence.hello.impl.entity.HelloEvent;
import com.lightbend.lagom.recipes.mixedpersistence.hello.impl.entity.HelloEvent.GreetingMessageChanged;
import org.pcollections.PSequence;
import com.lightbend.lagom.javadsl.persistence.jdbc.JdbcReadSide;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.inject.Singleton;
import javax.persistence.EntityManager;
import java.util.concurrent.CompletionStage;

@Singleton
public class Greetings {
//    private static final String SELECT_ALL_QUERY =
//            // JPA entities are mutable and cannot be safely shared across threads.
//            // The "SELECT NEW" syntax is used to return immutable result objects.
//            "SELECT NEW com.lightbend.lagom.recipes.mixedpersistence.hello.api.UserGreeting(g.id, g.message)" +
//                    " FROM UserGreetingRecord g";

    // JpaSession provides an asynchronous, non-blocking API to
    // perform JPA actions in Slick's database execution context.
    private final JdbcSession jdbcSession;

    @Inject
    Greetings(JdbcSession jdbcSession, ReadSide readSide) {
        this.jdbcSession = jdbcSession;

        // This registers an event processor with Lagom.
        // Event processors are used to update the read-side
        // database with changes made to persistent entities.
        readSide.register(UserGreetingRecordWriter.class);
    }

    /**
     * Asynchronously queries the read-side database for a list of all greetings that have been set for any HelloEntity.
     *
     * @return a {@link CompletionStage} that completes with a list of all stored greetings
     */
//    public CompletionStage<PSequence<UserGreeting>> all() {
//        return jpaSession
//                .withTransaction(this::selectAllUserGreetings)
//                .thenApply(TreePVector::from);
//    }

//    private List<UserGreeting> selectAllUserGreetings(EntityManager entityManager) {
//        return entityManager
//                .createQuery(SELECT_ALL_QUERY, UserGreeting.class)
//                .getResultList();
//    }

    /**
     * Event processor that handles {@link GreetingMessageChanged} events
     * by writing {@link UserGreetingRecord} rows to the read-side database table.
     */
    static class UserGreetingRecordWriter extends ReadSideProcessor<HelloEvent> {
      private final JdbcReadSide readSide;

        @Inject
        UserGreetingRecordWriter(JdbcReadSide readSide) {
            this.readSide = readSide;
        }

        @Override
        public ReadSideHandler<HelloEvent> buildHandler() {
//            return jdbcSession.<HelloEvent>builder("UserGreetingRecordWriter")
//                    .setGlobalPrepare(entityManager -> createTable())
//                    .setEventHandler(GreetingMessageChanged.class, this::processGreetingMessageChanged)
//                    .build();

          JdbcReadSide.ReadSideHandlerBuilder<HelloEvent> builder =
              readSide.builder("blogsummaryoffset");

          builder.setGlobalPrepare(this::createTable);
          builder.setEventHandler(GreetingMessageChanged.class, this::processPostAdded);

          return builder.build();
        }

//        private void createSchema() {
//            // This is a convenience for creating the read-side table in development mode.
//            // It relies on a Hibernate-specific property to provide idempotent schema updates.
//            Persistence.generateSchema("default",
//                    ImmutableMap.of("hibernate.hbm2ddl.auto", "update")
//            );
//        }

      private void createTable(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("CREATE TABLE IF NOT EXISTS blogsummary ( " +
            "id VARCHAR(64), title VARCHAR(256), PRIMARY KEY (id))")) {
          ps.execute();
        }
      }

        private void processGreetingMessageChanged(EntityManager entityManager, GreetingMessageChanged greetingMessageChanged) {
          System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^processGreetingMessageChanged");
          System.out.println(greetingMessageChanged);
//            UserGreetingRecord record = entityManager.find(UserGreetingRecord.class, greetingMessageChanged.getName());
//            if (record == null) {
//                record = new UserGreetingRecord();
//                record.setId(greetingMessageChanged.getName());
//                entityManager.persist(record);
//            }
//            record.setMessage(greetingMessageChanged.getMessage());

          UserGreetingRecord record = new UserGreetingRecord();
          record.setId(greetingMessageChanged.getName());
          record.setMessage(greetingMessageChanged.getMessage());
          entityManager.persist(record);
        }

      private void processPostAdded(Connection connection, GreetingMessageChanged event) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
            "INSERT INTO blogsummary (id, title) VALUES (?, ?)");
        statement.setString(1, event.getName());
        statement.setString(2, event.getMessage());
        statement.execute();
      }

        @Override
        public PSequence<AggregateEventTag<HelloEvent>> aggregateTags() {
            return HelloEvent.TAG.allTags();
        }
    }
}
