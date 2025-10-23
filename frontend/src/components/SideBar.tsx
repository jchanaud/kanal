import { Database, Map, Filter, GitBranch} from "lucide-react"
import { Card, CardContent } from "./ui/card"
import { memo } from "react"

const Sidebar = memo(() => {
  const onDragStart = (event: React.DragEvent, nodeType: string) => {
    event.dataTransfer.setData("application/reactflow", nodeType)
    event.dataTransfer.effectAllowed = "move"
    console.log("Drag started for node type:", nodeType)
  }

  return (
    <div className="w-64 bg-gray-50 border-r border-gray-200 p-4">
      <h3 className="font-semibold mb-4">Components</h3>
      
      {/* Sources & Sinks */}
      <div className="mb-6">
        <h4 className="text-sm font-medium text-gray-700 mb-2">Sources & Sinks</h4>
        <div className="space-y-2">
          <div
            className="cursor-grab active:cursor-grabbing"
            onDragStart={(event) => onDragStart(event, "kstream-input")}
            draggable
          >
            <Card className="border-green-200 bg-green-50 hover:bg-green-100 transition-colors">
              <CardContent className="p-3">
                <div className="flex items-center gap-2">
                  <Database className="w-4 h-4 text-green-600" />
                  <span className="text-sm font-medium">Source</span>
                </div>
                <div className="text-xs text-gray-500 mt-1">Kafka Topic Input</div>
              </CardContent>
            </Card>
          </div>

          <div
            className="cursor-grab active:cursor-grabbing"
            onDragStart={(event) => onDragStart(event, "topic-output")}
            draggable
          >
            <Card className="border-red-200 bg-red-50 hover:bg-red-100 transition-colors">
              <CardContent className="p-3">
                <div className="flex items-center gap-2">
                  <Database className="w-4 h-4 text-red-600" />
                  <span className="text-sm font-medium">Sink</span>
                </div>
                <div className="text-xs text-gray-500 mt-1">Kafka Topic Output</div>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>

      {/* Processors */}
      <div>
        <h4 className="text-sm font-medium text-gray-700 mb-2">Processors</h4>
        <div className="space-y-2">
          <div
            className="cursor-grab active:cursor-grabbing"
            onDragStart={(event) => onDragStart(event, "map")}
            draggable
          >
            <Card className="border-blue-200 bg-blue-50 hover:bg-blue-100 transition-colors">
              <CardContent className="p-3">
                <div className="flex items-center gap-2">
                  <Map className="w-4 h-4 text-blue-600" />
                  <span className="text-sm font-medium">Map</span>
                </div>
                <div className="text-xs text-gray-500 mt-1">Transform data</div>
              </CardContent>
            </Card>
          </div>

          <div
            className="cursor-grab active:cursor-grabbing"
            onDragStart={(event) => onDragStart(event, "filter")}
            draggable
          >
            <Card className="border-purple-200 bg-purple-50 hover:bg-purple-100 transition-colors">
              <CardContent className="p-3">
                <div className="flex items-center gap-2">
                  <Filter className="w-4 h-4 text-purple-600" />
                  <span className="text-sm font-medium">Filter</span>
                </div>
                <div className="text-xs text-gray-500 mt-1">Filter records</div>
              </CardContent>
            </Card>
          </div>

          <div
            className="cursor-grab active:cursor-grabbing"
            onDragStart={(event) => onDragStart(event, "branch")}
            draggable
          >
            <Card className="border-orange-200 bg-orange-50 hover:bg-orange-100 transition-colors">
              <CardContent className="p-3">
                <div className="flex items-center gap-2">
                  <GitBranch className="w-4 h-4 text-orange-600" />
                  <span className="text-sm font-medium">Branch</span>
                </div>
                <div className="text-xs text-gray-500 mt-1">Conditional routing</div>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  )
});

export default Sidebar;