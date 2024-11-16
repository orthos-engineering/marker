from marker.v2.schema import BlockTypes
from marker.v2.schema.blocks import Block


class Picture(Block):
    block_type: BlockTypes = BlockTypes.Picture

    def assemble_html(self, child_blocks, parent_structure):
        return f"Image {self.block_id}"
