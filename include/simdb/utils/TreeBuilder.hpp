// <TreeBuilder> -*- C++ -*-

#pragma once

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace simdb
{

struct TreeNode {
    std::string name;
    std::vector<std::unique_ptr<TreeNode>> children;
    TreeNode* parent = nullptr;

    TreeNode(const std::string& name, TreeNode* parent = nullptr)
        : name(name)
        , parent(parent)
    {
    }
};

// Function to split_string a string by a delimiter
inline std::vector<std::string> split_string(const std::string& s, char delimiter)
{
    std::vector<std::string> tokens;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delimiter)) {
        tokens.push_back(item);
    }
    return tokens;
}

/*!
 * \brief Build a tree from a list of strings.
 *
 * \param tree_paths A list of strings that represent
 *                   the tree structure. Each string should
 *                   be a path to a node in the tree, with
 *                   each node separated by a period.
 *                   For example, "parent1.child2".
 *
 * \return A unique pointer to the root node of the tree.
 */
inline std::unique_ptr<TreeNode> buildTree(std::vector<std::string> tree_paths)
{
    // Remove any path that is just named "root" as it will be created automatically.
    tree_paths.erase(std::remove_if(tree_paths.begin(), tree_paths.end(), [](const std::string& path) { return path == "root"; }),
                     tree_paths.end());

    // If all paths start with "root.", remove it so we do not end up with
    // tree node paths that start with "root.root".
    if (std::all_of(tree_paths.begin(), tree_paths.end(), [](const std::string& path) { return path.find("root.") == 0; })) {
        for (auto& path : tree_paths) {
            path = path.substr(5);
        }
    }

    std::unique_ptr<TreeNode> root(new TreeNode("root"));

    for (const auto& tree_string : tree_paths) {
        auto path = split_string(tree_string, '.');
        auto node = root.get();

        for (const auto& name : path) {
            auto found = std::find_if(node->children.begin(), node->children.end(), [&name](const std::unique_ptr<TreeNode>& child) {
                return child->name == name;
            });

            if (found == node->children.end()) {
                std::unique_ptr<TreeNode> new_node(new TreeNode(name, node));
                node->children.push_back(std::move(new_node));
                node = node->children.back().get();
            } else {
                node = found->get();
            }
        }
    }

    return root;
}

} // namespace simdb
