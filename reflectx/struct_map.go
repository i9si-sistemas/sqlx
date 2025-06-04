package reflectx

// A StructMap is an index of field metadata for a struct.
type StructMap struct {
	Tree  *FieldInfo
	Index []*FieldInfo
	Paths map[string]*FieldInfo
	Names map[string]*FieldInfo
}

// GetByPath returns a *FieldInfo for a given string path.
func (f StructMap) GetByPath(path string) *FieldInfo {
	return f.Paths[path]
}

// GetByTraversal returns a *FieldInfo for a given integer path.  It is
// analogous to reflect.FieldByIndex, but using the cached traversal
// rather than re-executing the reflect machinery each time.
func (f StructMap) GetByTraversal(index []int) *FieldInfo {
	if len(index) == 0 {
		return nil
	}

	tree := f.Tree
	for _, i := range index {
		if i >= len(tree.Children) || tree.Children[i] == nil {
			return nil
		}
		tree = tree.Children[i]
	}
	return tree
}
