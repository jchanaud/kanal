import { useState, useCallback, useRef } from 'react';
import { ReactFlow, applyNodeChanges, applyEdgeChanges, addEdge, MiniMap, type NodeTypes, ConnectionLineType, Background, BackgroundVariant, type Edge, type Node, useReactFlow, MarkerType, type NodeChange, type EdgeChange, type OnConnect, type OnConnectStart } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import MapNode from './nodes/MapNode';
import KStreamInputNode from './nodes/KStreamInputNode';
import TopicOutputNode from './nodes/TopicOutputNode';
import PropertiesPanel from './PropertiesPanel';
import Sidebar from './SideBar';
import MappingEditor from './MappingEditor';
import BranchNode from './nodes/BranchNode';
import ContextMenu from './ContextMenu';
import type { NodeData } from './nodes/BaseNode';

const initialNodes: Node[] = [
  { id: 'n3', type: 'kstream-input', position: { x: 200, y: 75 }, data: {} },
  { id: 'n4', type: 'map', position: { x: 500, y: 83 }, data: { label: 'Node 4' } },
  { id: 'n5', type: 'topic-output', position: { x: 800, y: 75 }, data: { context: 'too' } },
];
const initialEdges: Edge[] = [{ id: 'n1-n2', source: 'n1', target: 'n2' }];
const nodeTypes: NodeTypes = {
  'kstream-input': KStreamInputNode,
  'map': MapNode,
  'topic-output': TopicOutputNode,
  'branch': BranchNode
};
// Define nodeColor function for MiniMap
function nodeColor(node: Node) {
  switch (node.type) {
    case 'kstream-input':
      return '#6ede87';
    case 'map':
      return '#6865A5';
    case 'topic-output':
      return '#ff0072';
    case 'branch':
      return '#FFA500';
    default:
      return '#eee';
  }
}




export function Flow() {
  const reactFlowWrapper = useRef(null);
  const { screenToFlowPosition } = useReactFlow();
  const [nodes, setNodes] = useState(initialNodes);
  const [edges, setEdges] = useState(initialEdges);
  const [selectedNode, setSelectedNode] = useState<Node | null>(null)
  const [selectedEdge, setSelectedEdge] = useState<Edge | null>(null)
  const [showMappingEditor, setShowMappingEditor] = useState(false)
  const [menu, setMenu] = useState(null);



  const onNodesChange = useCallback((changes: NodeChange<Node>[]) => {
    setNodes((nds) => applyNodeChanges(changes, nds));
  }, []);

  const onEdgesChange = useCallback((changes: EdgeChange<Edge>[]) => {
    setEdges((eds) => applyEdgeChanges(changes, eds));
  }, []);

  const onConnect: OnConnect = useCallback((connection) => {
    setEdges((eds) => addEdge({ ...connection, style: { strokeWidth: 2 }, markerEnd: { type: MarkerType.ArrowClosed }, 
    }, eds));
  }, []);
  const onConnectStart: OnConnectStart = useCallback((_event, { nodeId, handleId, handleType }) => {
    console.log('Connection started from node: %s with handle: %s, type: %s', nodeId, handleId, handleType);
    const type = handleId ? handleId.split(':')[1] : undefined;

    setNodes((nds) =>
      nds.map((n) => ({ ...n, data: { ...n.data, handlePicked: { dataType: type, handleType, nodeId, handleId } } }))
    );
  }, []);
  const onConnectEnd = () => {
    setNodes((nds) =>
      nds.map((n) => ({ ...n, data: { ...n.data, handlePicked: { dataType: null, handleType: null, nodeId: null, handleId: null } } }))
    );
  };
  const onNodeClick = useCallback((_event: React.MouseEvent, node: Node) => {
    setSelectedNode(node)
    setSelectedEdge(null)
    console.log('Node clicked:', node);
  }, [])

  const onEdgeClick = useCallback((_event: React.MouseEvent, edge: Edge) => {
    setSelectedEdge(edge)
    setSelectedNode(null)
  }, []);

  const onPaneClick = useCallback(() => {
    setSelectedNode(null)
    setSelectedEdge(null)
    setMenu(null)
  }, []);

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault()
    event.dataTransfer.dropEffect = "move"
  }, [])

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault()
      const type = event.dataTransfer.getData("application/reactflow")
      console.log("onDrop type:", type)
      if (typeof type === "undefined" || !type) {
        return
      }
      const position = screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      });
      const newNode: Node = {
        id: `${type}-${Date.now()}`,
        type,
        position,
        data: {
          label: type.charAt(0).toUpperCase() + type.slice(1),
          topic: type === "source" ? "input-topic" : type === "sink" ? "output-topic" : undefined,
          conditions: [
    { id: "1", condition: "value > 100", label: "High" },
    { id: "2", condition: "value <= 100", label: "Low" },
    { id: "3", condition: "value === 100", label: "Equal" },
    { id: "4", condition: "value !== 100", label: "Not Equal" },
    { id: "5", condition: "value < 100", label: "Less Than" },
  ],
          onNodeContextMenu: onNodeContextMenu,
        },
      }

      setNodes((nds) => nds.concat(newNode))
    },
    [setNodes],
  )

  const updateNode = (id: string, data: NodeData) => {
    setNodes((nds) => nds.map((n) => (n.id === id ? { ...n, data: { ...n.data, ...data } } : n)));
  };
  const onNodeContextMenu = useCallback(
      (event, nodeId, handle) => {
        // Prevent native context menu from showing
        event.preventDefault();
        console.log('Context menu for node:', nodeId);
        // Calculate position of the context menu. We want to make sure it
        // doesn't get positioned off-screen.
        console.log('Mouse at:', event.clientX, event.clientY);
        const pane = reactFlowWrapper.current.getBoundingClientRect();
        console.log('Pane dimensions:', pane);
        setMenu({
          id: nodeId,
          handle: handle,
          top: event.clientY < pane.height - 200 && event.clientY - pane.y,
          left: event.clientX < pane.width - 200 && event.clientX - pane.x,
          right: event.clientX >= pane.width - 200 && pane.width - event.clientX,
          bottom: event.clientY >= pane.height - 200 && pane.height - event.clientY,
        });
      },
      [setMenu],
    );
  return (
    <div className="flex h-dvh">
      <Sidebar />
      <div className="flex-1" ref={reactFlowWrapper}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onConnectStart={onConnectStart}
          onConnectEnd={onConnectEnd}
          onNodeClick={onNodeClick}
          onEdgeClick={onEdgeClick}
          onPaneClick={onPaneClick}
          onDrop={onDrop}
          onDragOver={onDragOver}
          nodeTypes={nodeTypes}
          connectionLineType={ConnectionLineType.Bezier}
          connectionLineStyle={{ strokeWidth: 2, markerEnd: MarkerType.ArrowClosed }}
        >
          <MiniMap
            nodeStrokeWidth={3}
            nodeColor={nodeColor}
            zoomable
            pannable
          />
          <Background variant={BackgroundVariant.Dots} size={1} />
          {menu && <ContextMenu onClick={onPaneClick} {...menu} />}
        </ReactFlow>
      </div>
      {(selectedNode || selectedEdge) && (
        <PropertiesPanel
          key={selectedNode?.id || selectedEdge?.id}
          selectedNode={selectedNode}
          selectedEdge={selectedEdge}
          onUpdateNode={updateNode}
          onClose={() => {
            setSelectedNode(null)
            setSelectedEdge(null)
          }}
          onOpenMappingEditor={() => { setShowMappingEditor(true) }}
        />
      )}
      <MappingEditor
        isOpen={showMappingEditor}
        onClose={() => setShowMappingEditor(false)}
        onSave={(data) => {
          // TODO: Implement save logic
          console.log('Mapping data saved:', data);
        }}
        nodeData={selectedNode?.data || {}}
      />
    </div>
  );
}
