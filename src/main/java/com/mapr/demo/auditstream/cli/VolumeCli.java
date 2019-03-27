package com.mapr.demo.auditstream.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.demo.auditstream.db.entity.Volume;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
public class VolumeCli {
    private final static String VOLUME_CLI_COMMAND = "maprcli volume list -columns volumename,volumeid,mountdir -json";
    private final static ObjectMapper MAPPER = new ObjectMapper();

    public static List<Volume> getAllVolumes() {
        return parse(run(VOLUME_CLI_COMMAND));
    }

    public static Optional<Volume> getVolumeByName(String name) {
        String command = String.format("%s -filter [%s==%s]", VOLUME_CLI_COMMAND, "volumename", name);
        return parse(run(command)).stream().findFirst();
    }

    public static Optional<Volume> getVolumeById(Integer id) {
        String command = String.format("%s -filter [%s==%d]", VOLUME_CLI_COMMAND, "volumeid", id);
        return parse(run(command)).stream().findFirst();
    }

    @SneakyThrows
    static String run(String command) {
        log.debug("Running: {}", command);
        Process process = Runtime.getRuntime().exec(command);
        BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
        StringBuilder builder = new StringBuilder();

        String line;
        while ((line = in.readLine()) != null) {
            builder.append(line);
        }

        String result = builder.toString();
        log.debug("Result:\n{}", result);

        return result;
    }

    @SneakyThrows
    static List<Volume> parse(String line) {
        Iterator<JsonNode> iterator = MAPPER.readTree(line).withArray("data").elements();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .map(VolumeCli::convertToVolume).collect(Collectors.toList());
    }

    static Volume convertToVolume(JsonNode node) {
        Volume volume = new Volume();

        volume.setId(node.get("volumeid").asText());
        volume.setName(node.get("volumename").asText());
        volume.setPath(node.get("mountdir").asText());

        return volume;
    }

}
