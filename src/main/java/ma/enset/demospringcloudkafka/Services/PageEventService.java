package ma.enset.demospringcloudkafka.Services;

import ma.enset.demospringcloudkafka.entities.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Service
public class PageEventService {

    @Bean
    public Consumer<PageEvent> pageEventConsumer(){
        return (input)->{
            System.out.println("************************");
            System.out.println(input.toString());
            System.out.println("************************");
        };
    }
    @Bean
    public Supplier<PageEvent> pageEventSupplier(){
        return ()-> new PageEvent(
                
                Math.random()>0.5?"P1":"P2" ,
                Math.random()>0.5?"U1":"U2" ,
                new Date() ,
                new Random().nextInt());
    }

    //@Bean
    public Consumer<KStream<String,PageEvent>> pageStreamConsumer(){
        return (pageEvent -> pageEvent
                .filter((k,v)->v.getDuration()>100)
                .map((k,v)->new KeyValue<>(v.getName(),v))
                .peek((k,v)-> System.out.println(k+"=>"+v))
                .groupByKey(Grouped.with(Serdes.String(), AppSerdes.PageEventSerdes()))
                .reduce((acc,v)->{
                    v.setDuration(v.getDuration()+acc.getDuration());
                    return v;
                })
                .toStream()
                .peek((k,v)-> System.out.println(k+"=>"+v)));
    }
}
