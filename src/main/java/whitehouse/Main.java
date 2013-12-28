package whitehouse;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.buffer.FirstNBuffer;
import cascading.operation.Insert;
import cascading.pipe.Pipe;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.assembly.Retain;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class Main {
    public final static int TOP_N = 20;

    public static void main(String[] args) {
        String visitsPath = args[0];
        String vcPath = args[1];

        Properties properties = new Properties();
        AppProps.setApplicationJarClass( properties, Main.class );
        HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );

        // create source and sink taps
        Tap visitsTap = new Hfs( new TextDelimited(true, ",", "\""), visitsPath);
        Tap vcTap = new Hfs(new TextDelimited(true, "\t", "\""), vcPath);
        
        // count occurences of each visitor
        Pipe vcPipe = new Pipe("visitor_count");
        Fields visitorName = new Fields("namefirst", "namelast");
        Fields visitCount = new Fields("visits_count");
        vcPipe = new Retain(vcPipe, visitorName);
        vcPipe = new GroupBy(vcPipe, visitorName);
        vcPipe = new Every(vcPipe, Fields.ALL, new Count(visitCount), Fields.ALL);
        
        // output top N
        Fields identity = new Fields("identity"); 
        Fields visitorcount = new Fields("namefirst", "namelast", "visits_count");
        vcPipe = new Each(vcPipe, new Insert(identity, 1), Fields.ALL);
        vcPipe = new GroupBy(vcPipe, identity, visitCount, true);
        vcPipe = new Every(vcPipe, visitorcount, new FirstNBuffer(TOP_N), Fields.RESULTS);
        
        
        // connect taps and pipes into a flow
        FlowDef flowDef = FlowDef.flowDef()
            .setName("vc")
            .addSource(vcPipe, visitsTap)
            .addTailSink(vcPipe, vcTap);

        // write a DOT file and run the flow
        Flow vcFlow = flowConnector.connect(flowDef);
        vcFlow.writeDOT("dot/vc.dot");
        vcFlow.complete();
    }
}
