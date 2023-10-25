class Node:
    def __init__(self, children: None, **kwargs):
        self.children = children if children else []
        self.attributes = kwargs

    def add_as_child(self, child):
        self.children.append(child);

    def visualize_parsed_result(self):
        # plot somethings
        pass