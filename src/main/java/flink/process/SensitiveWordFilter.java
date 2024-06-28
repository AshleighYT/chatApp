package flink.process;

import model.KafkaEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class SensitiveWordFilter implements MapFunction<KafkaEvent, KafkaEvent> {
    private static final Set<String> SENSITIVE_WORDS = new HashSet<>();
//    private static final Logger LOG = LoggerFactory.getLogger(SensitiveWordFilter.class);

    static {
        try {
            InputStream inputStream = SensitiveWordFilter.class.getClassLoader().getResourceAsStream("wordlist.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                SENSITIVE_WORDS.add(line.trim().toLowerCase());
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public KafkaEvent map(KafkaEvent event) throws Exception {
        return censorMessage(event);
    }

    private KafkaEvent censorMessage(KafkaEvent event) {
        String message = event.getMessage();
        boolean containsSensitiveWord = false;

        Pattern pattern = Pattern.compile("\\b(\\w+)\\b");
        Matcher matcher = pattern.matcher(message);

        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String word = matcher.group();
            String processedWord = word;

            if (SENSITIVE_WORDS.contains(word.toLowerCase())) {
                containsSensitiveWord = true;
                processedWord = censorSensitiveWord(word);
            }
            matcher.appendReplacement(sb, Matcher.quoteReplacement(processedWord));
        }
        matcher.appendTail(sb);

        event.setMessage(sb.toString());
        event.setSensitive(containsSensitiveWord);
        return event;
    }

    private String censorSensitiveWord(String word) {
        int length = word.length();
        if (length > 2) {
            return word.charAt(0) + "*".repeat(length - 2) + word.charAt(length - 1);
        } else {
            return "*".repeat(length);
        }
    }
}