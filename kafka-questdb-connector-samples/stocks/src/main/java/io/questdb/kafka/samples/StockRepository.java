package io.questdb.kafka.samples;


import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.jdbc.repository.support.SimpleJdbcRepository;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StockRepository extends CrudRepository<Stock, String> {

    @Modifying
    @Query("UPDATE stock SET price = price * :factor WHERE symbol = :symbol")
    boolean updateBySymbol(@Param("symbol") String symbol, @Param("factor") double factor);

    @Query("SELECT symbol FROM stock")
    List<String> findAllSymbols();
}
