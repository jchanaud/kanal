
import { Button } from "@/components/ui/button";
import type { Edge, Node } from "@xyflow/react";
import { Edit, Plus, X } from "lucide-react";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { useEffect, useState } from "react";
import { Textarea } from "./ui/textarea";
import { Card } from "./ui/card";
import type { NodeData } from "./nodes/BaseNode";

export default function PropertiesPanel({selectedNode, selectedEdge, onUpdateNode, onOpenMappingEditor, onClose}: {selectedNode: Node | null; selectedEdge: Edge | null; onUpdateNode: (id: string, data: NodeData) => void; onOpenMappingEditor: () => void; onClose: () => void;}) {

    const [label, setLabel] = useState<string>(
      typeof selectedNode?.data?.label === "string" ? selectedNode.data.label : ""
    );
    const [conditions, setConditions] = useState(selectedNode?.data?.conditions || []);

  useEffect(() => {
    if (selectedNode) {
      console.log("Updating node:", { ...selectedNode, data: { label: label, conditions: conditions } });
      onUpdateNode(selectedNode.id, { ...selectedNode.data, label: label, conditions: conditions });
    }
  }, [label, conditions]);

  const handleAddCondition = () => {
    // newId is largest id +1
    const newId = (conditions.reduce((max, condition) => Math.max(max, parseInt(condition.id)), 0) + 1).toString();
    setConditions((prev) => [
      { id: newId, condition: "", label: "New" },
      ...prev
      
    ]);
  };
  const handleRemoveCondition = (id: string) => {
    console.log("Removing condition with id:", id);
    setConditions((prev) => prev.filter((condition) => condition.id !== id));
  }
  const handleUpdateCondition = (id: string, field: string, value: string) => {
    setConditions((prev) =>
      prev.map((condition) =>
        condition.id === id ? { ...condition, [field]: value } : condition
      )
    );
  };

  return (
    <div className="w-80 bg-white border-l border-gray-200 p-4 overflow-y-auto">
      <div className="flex items-center justify-between mb-4">
        <h3 className="font-semibold">
          {selectedEdge ? 'Edge Properties' : `${selectedNode?.type} Properties`}
        </h3>
        <Button variant="ghost" size="sm" onClick={onClose}>
          <X className="w-4 h-4" />
        </Button>
      </div>
      {selectedNode && (
        <div className="space-y-4">
          <div className="grid w-full gap-3">
            <Label htmlFor="label">Label</Label>
            <Input
              id="label"
              value={label}
              onChange={(e) => setLabel(e.target.value)}
              placeholder="Enter label"
            />
          </div>
          {(selectedNode.type === "kstream-input" || selectedNode.type === "topic-output") && (
            <>
            <div className="grid w-full gap-3">
            <Label htmlFor="label">Topic</Label>
            <Input
              id="label"
              value="topic"
              placeholder="Enter label"
            />
          </div>
            </>
          )}


          {selectedNode.type === "map" && (
            <>
            <div className="grid w-full gap-3">
              <Label htmlFor="mapping">Mapping</Label>
              <Textarea
                id="mapping"
                placeholder={
`{
  "id": id,
  "email": email,
  "recentOrders": orders.{
    "id": orderId
  }
}`}
              />
              
            </div>
            <Button onClick={onOpenMappingEditor} className="w-full bg-blue-600 hover:bg-blue-700">
                <Edit className="w-4 h-4 mr-2" />
                Edit Mapping
              </Button>
            </>
          )}

          {selectedNode.type === "branch" && (
            <div>
              <div className="flex items-center justify-between mb-2">
                <Label>Conditions</Label>
                <Button variant="outline" size="sm" onClick={handleAddCondition}>
                  <Plus className="w-4 h-4 mr-1" />
                  Add
                </Button>
              </div>
              <div className="space-y-3">
                {(conditions || []).map((condition) => (
                  <Card key={condition.id} className="p-3">
                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <Label className="text-xs">Branch Label</Label>
                        <Button variant="ghost" size="sm" onClick={() => handleRemoveCondition(condition.id)}>
                          <X className="w-3 h-3" />
                        </Button>
                      </div>
                      <Input
                        value={condition.label}
                        onChange={(e) => handleUpdateCondition(condition.id, "label", e.target.value)}
                        placeholder="Branch name"
                        className="text-xs"
                      />
                      <Label className="text-xs">Condition</Label>
                      <Textarea
                        value={condition.condition}
                        onChange={(e) => handleUpdateCondition(condition.id, "condition", e.target.value)}
                        placeholder="e.g., value > 100"
                        className="text-xs"
                        rows={2}
                      />
                    </div>
                  </Card>
                ))}
              </div>
            </div>
          )}

        </div>
      )}
    </div>
  );
}
