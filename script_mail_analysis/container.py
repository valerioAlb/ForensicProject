# coding=utf-8


class Container:

    def __init__(self):
        self.message = self.parent = None
        self.children = []

    def remove_child(self, child):
        self.children.remove(child)
        # Remove the child, and remove the parent-link
        child.parent = None

    def add_child(self, child):
        # If child has a parent remove! the new one is self! Remove also the child from its parent to destroy
        # all the previous links.
        if child.parent:
            child.parent.remove_child(child)
        child.parent = self
        self.children.append(child)

    def introduce_loop(self, rfr_container):

        # Check all children of the container, and recursively all sub-children.
        container_list = []
        container_list.append(self)
        visited = []

        while len(container_list) > 0:

            extracted_container = container_list.pop()

            if extracted_container is rfr_container:
                return True

            else:
                visited.append(extracted_container)
                for child in extracted_container.children:
                    if child not in visited:
                        container_list.append(child)

        return False
