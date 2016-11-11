package conductor

import (
	"fmt"
	"io/ioutil"

	"github.com/awalterschulze/gographviz"
)

func (t *Topology) Graph(file string) {
	graph := gographviz.NewGraph()

	graph.SetName(t.name)
	graph.SetDir(true)
	graph.AddAttr(t.name, "rankdir", "LR")
	graph.AddAttr(t.name, "ranksep", "0.7")
	graph.AddAttr(t.name, "nodesep", "0.7")
	graph.AddAttr(t.name, "splines", "spline")

	for _, s := range t.streams {
		for _, p := range s.producers {
			for _, c := range s.consumers {
				graph.AddPortEdge(p.operatorName, p.operatorName+":o"+s.name, c.operatorName, c.operatorName+":i"+s.name, true, nil)
			}
		}
	}

	for _, o := range t.spouts {
		graph.Nodes.Add(createGraphNode(nil, o.name, o.parallelism, o.outputs))
	}

	for _, o := range t.bolts {
		graph.Nodes.Add(createGraphNode(o.inputs, o.name, o.parallelism, o.outputs))
	}

	ioutil.WriteFile(file, []byte(graph.String()), 0644)
}

func createGraphNode(inputs []*inputPort, name string, parallelism int, outputs []*outputPort) *gographviz.Node {
	var iports []string
	for _, ip := range inputs {
		iports = append(iports, fmt.Sprintf("<TD PORT=\"i%s\">%s</TD>", ip.streamName, ip.streamName))
	}

	var oports []string
	for _, op := range outputs {
		oports = append(oports, fmt.Sprintf("<TD PORT=\"o%s\">%s</TD>", op.streamName, op.streamName))
	}

	rows := len(iports)
	if rows < len(oports) {
		rows = len(oports)
	}

	operatorRow := fmt.Sprintf("<TR><TD COLSPAN=\"2\" BGCOLOR=\"#000000\"><FONT COLOR=\"#FFFFFF\">%s (%d)</FONT></TD></TR>", name, parallelism)
	headerRow := "<TR><TD BGCOLOR=\"#DDDDDD\"><I>input ports</I></TD><TD BGCOLOR=\"#DDDDDD\"><I>output ports</I></TD></TR>"
	portRows := ""
	for row := 0; row < rows; row++ {
		portRows = portRows + "<TR>"

		if row < len(inputs) {
			portRows = portRows + iports[row]
		} else if row == len(inputs) {
			portRows = portRows + fmt.Sprintf("<TD ROWSPAN=\"%d\"></TD>", rows-row)
		}

		if row < len(outputs) {
			portRows = portRows + oports[row]
		} else if row == len(outputs) {
			portRows = portRows + fmt.Sprintf("<TD ROWSPAN=\"%d\"></TD>", rows-row)
		}
		portRows = portRows + "</TR>"
	}

	label := fmt.Sprintf("<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">%s%s%s</TABLE>>", operatorRow, headerRow, portRows)

	return &gographviz.Node{
		Name: name,
		Attrs: map[string]string{
			"shape": "plaintext",
			"label": label,
		},
	}
}
