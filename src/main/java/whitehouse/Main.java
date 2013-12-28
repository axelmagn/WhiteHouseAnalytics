package whitehouse;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.buffer.FirstNBuffer;
import cascading.operation.Insert;
import cascading.pipe.Checkpoint;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
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
        String dateJoinCheckPath = args[2];

        Properties properties = new Properties();
        AppProps.setApplicationJarClass( properties, Main.class );
        HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );

        // create source and sink taps
        Tap visitsTap = new Hfs( new TextDelimited(true, ",", "\""), visitsPath);
        Tap vcTap = new Hfs(new TextDelimited(true, "\t", "\""), vcPath);
        Tap visitorPairCountTap = new Hfs(new TextDelimited(true, "\t", "\""), dateJoinCheckPath);
        
        Pipe visitPipe = new Pipe("visit_pipe");
        Fields visitFields = new Fields("namefirst", "namelast", "appt_start_date");
        visitPipe = new Retain(visitPipe, visitFields);

        // count occurences of each visitor
        Fields visitorName = new Fields("namefirst", "namelast");
        Fields visitCount = new Fields("visits_count");
        Fields date = new Fields("appt_start_date");

        Pipe vcPipe = new Pipe("visitor_count");
        vcPipe = new GroupBy(visitPipe, visitorName);
        vcPipe = new Every(vcPipe, Fields.ALL, new Count(visitCount), Fields.ALL);
        
        // output top N
        Fields identity = new Fields("identity"); 
        Fields visitorcount = new Fields("namefirst", "namelast", "visits_count");
        vcPipe = new Each(vcPipe, new Insert(identity, 1), Fields.ALL);
        vcPipe = new GroupBy(vcPipe, identity, visitCount, true);
        vcPipe = new Every(vcPipe, visitorcount, new FirstNBuffer(TOP_N), Fields.RESULTS);

        // join visitors on same date
        Fields leftVisit = new Fields("leftnamefirst", "leftnamelast", "left_appt_start_date");
        Fields rightVisit = new Fields("rightnamefirst", "rightnamelast", "right_appt_start_date");
        Fields leftVisitor = new Fields("leftnamefirst", "leftnamelast");
        Fields rightVisitor = new Fields("rightnamefirst", "rightnamelast");
        Fields leftDate = new Fields("left_appt_start_date");
        Fields rightDate = new Fields("right_appt_start_date");
        Pipe leftVisitsPipe = new Pipe("left_visits", visitPipe);
        Pipe rightVisitsPipe = new Pipe("right_visits", visitPipe);
        Pipe visitorDateJoin = new Pipe("visitor_date_join");
        leftVisitsPipe = new Rename(leftVisitsPipe, visitFields, leftVisit);
        rightVisitsPipe = new Rename(rightVisitsPipe, visitFields, rightVisit);
        visitorDateJoin = new CoGroup(leftVisitsPipe, leftDate, rightVisitsPipe, rightDate, new InnerJoin());

        // count visitor pairings
        Fields visitorPair = new Fields("leftnamefirst", "leftnamelast", "rightnamefirst", "rightnamelast");
        Fields count = new Fields("visitor_pair_count");
        Pipe visitorPairCount = new Pipe("visitor_pair_count", visitorDateJoin);
        visitorPairCount = new CountBy(visitorPairCount, visitorPair, count);
        
        // connect taps and pipes into a flow
        FlowDef flowDef = FlowDef.flowDef()
            .setName("visit_analysis")
            .addSource(vcPipe, visitsTap)
            .addTailSink(vcPipe, vcTap)
            .addTailSink(visitorPairCount, visitorPairCountTap);

        // write a DOT file and run the flow
        Flow vcFlow = flowConnector.connect(flowDef);
        vcFlow.writeDOT("dot/vc.dot");
        vcFlow.complete();
    }
}
