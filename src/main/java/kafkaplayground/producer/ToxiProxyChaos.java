package kafkaplayground.producer;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.io.IOException;

public class ToxiProxyChaos {
    public static void main(String[] args) throws IOException, InterruptedException {
        ToxiproxyClient client = new ToxiproxyClient("localhost", 8474);
        Proxy kafka1Proxy = client.getProxy("kafka1");
        try {
            kafka1Proxy.toxics().get("timeout").remove();
        } catch (Exception e) {
            System.out.println("No timeout toxic found");
        }
        for (int i = 0; i < 10; i++) {
            System.out.println("Adding timeout toxic");
            Toxic toxic = kafka1Proxy.toxics().timeout("timeout", ToxicDirection.DOWNSTREAM, 0);
            Thread.sleep(50000);
            System.out.println("Removing timeout toxic");
            toxic.remove();
            Thread.sleep(5000);
        }
    }
}
