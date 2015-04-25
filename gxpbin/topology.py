#!/usr/bin/env python

####################################
# Network Topology XML File Parser
####################################

import sys, os, re
import xml.dom.minidom

# XML Specific Tags
TAG_GRID    = "GRID"
TAG_CLUSTER = "CLUSTER"
TAG_SWITCH  = "SWITCH"
TAG_HOST    = "HOST"
TAG_ID      = "id"
TAG_BW      = "bw"

class TopParser:
    def __init__(self, xmlfile, Ws = None, Es = None):
        if Ws is None:
            self.Ws = sys.stdout.write
        if Es is None:
            self.Es = sys.stderr.write

        if not os.path.exists(xmlfile):
           self.Es("error: file %s does not exist\n" % xmlfile)
           return None
        
        # XML Structures
        self.xmlFile = xmlfile
        self.xmlDoc = xml.dom.minidom.parse(self.xmlFile)
        self.xmlGNodes = \
            self.xmlDoc.getElementsByTagName(TAG_GRID)
        self.xmlCNodes = \
            self.xmlDoc.getElementsByTagName(TAG_CLUSTER)
        self.xmlSNodes = \
            self.xmlDoc.getElementsByTagName(TAG_SWITCH)
        self.xmlHNodes = \
            self.xmlDoc.getElementsByTagName(TAG_HOST)
        
        # Name lists
        self.grid = map(lambda g:g.getAttribute(TAG_ID), \
            self.xmlGNodes)[0]
        self.clusters = map(lambda c:c.getAttribute(TAG_ID), \
            self.xmlCNodes)
        self.switches = map(lambda s:s.getAttribute(TAG_ID), \
            self.xmlSNodes)
        self.hosts = map(lambda h:h.getAttribute(TAG_ID), \
            self.xmlHNodes)

        # Name -> XML Mappings
        self.map = {}
        for g in self.xmlGNodes:
            self.map[g.getAttribute(TAG_ID)] = g
        for c in self.xmlCNodes:
            self.map[c.getAttribute(TAG_ID)] = c
        for s in self.xmlSNodes:
            self.map[s.getAttribute(TAG_ID)] = s
        for h in self.xmlHNodes:
            self.map[h.getAttribute(TAG_ID)] = h
    
    def get_grid(self):
        """
        return: Name of the Grid
        """
        return self.grid
    
    def get_clusters(self):
        """
        return: List of clusters' names
        """
        return self.clusters
    
    def get_switches(self, locations):
        """
        return: List of switches' names
        locations: String of regular expression of site 
                   name, seperated by colon
        """
        sitelist = locations.split(":")
        assert sitelist != []
        if self.get_grid() in sitelist: return self.switches
        
        swlist = []
        for s in sitelist:
            regexp = re.compile(s)
            for sw in self.switches:
                if regexp.match(sw): swlist.append(sw)

        return swlist

    def get_hosts(self, locations):
        """
        return: List of hosts' names
        locations: String of regular expression of site 
                   name, seperated by colon
        """
        sitelist = locations.split(":")
        if self.get_grid() in sitelist: return self.hosts

        hostlist = []
        for s in sitelist:
            regexp = re.compile(s)
            for h in self.hosts:
                if regexp.match(h): hostlist.append(h)

        return hostlist

    def get_sites(self, locations):
        """
        return: List of switches, hosts names
        locations: String of regular expression of site 
                   name, seperated by colon
        """
        sites = self.get_switches(locations) + \
                self.get_hosts(locations)
        sites.sort()
        return sites
    
    def bandwidth_of(self, node):
        """
        return: Weight of node (switch and host)
        """
        try:
            return self.map[node].getAttribute(TAG_BW)
        except KeyError:
            self.Es("error: %s does not exist\n" % node)

    def parent_of(self, node):
        """
        return: Parent of node
        """
        if node == self.grid:
            self.Es("error: Top node does not have parent\n")
            return None
        try:
            return self.map[node].parentNode.getAttribute(TAG_ID)
        except KeyError:
            self.Es("error: %s does not exist\n" % node)

    def children_of(self, node, type = None):
        """
        return: Children of node of type
        """
        children = []
        if type is None:
            types = [ TAG_CLUSTER, TAG_HOST, TAG_SWITCH ]
        else:
            types = [ type.upper() ]

        for c in self.map[node].childNodes:
            if c.nodeName in types:
                children.append(c)

        return map(lambda c:c.getAttribute(TAG_ID), children)
   
    def sibling_of(self, node, type = None):
        """
        return: Sibling of node of type
        """
        sibling = []
        if type is None:
            types = [ TAG_CLUSTER, TAG_HOST, TAG_SWITCH ]
        else:
            types = [ type.upper() ]
        
        for c in self.map[node].parentNode.childNodes:
            if c.nodeName in types:
                sibling.append(c)
        
        s = map(lambda c:c.getAttribute(TAG_ID), sibling)
        if node in s: s.remove(node)
        return s
    
    def ancestors_of(self, node, type = None):
        """
        return: Ancestor of give type of node
        """
        ancestors = []
        if type is None:
            types = [ TAG_GRID, TAG_CLUSTER, TAG_HOST, TAG_SWITCH ]
        else:
            types = [ type.upper() ]

        n = self.map[node]
        while True:
            p = n.parentNode
            if p.nodeName in types:
                ancestors.append(p)
            if p.getAttribute(TAG_ID) == self.grid:
                break
            n = p

        return map(lambda c:c.getAttribute(TAG_ID), ancestors)

    def nca_of(self, node1, node2):
        """
        return: Nearest common ancetor of node1 and node2
        """
        ancestors1 = self.ancestors_of(node1)
        ancestors2 = self.ancestors_of(node2)
        # Add self to ancestors set if not host node
        if self.map[node1].nodeName != TAG_HOST:
            ancestors1.insert(0, node1)
        if self.map[node2].nodeName != TAG_HOST:
            ancestors2.insert(0, node2)
        for nca in ancestors1:
            if nca in ancestors2:
                return nca
        self.Es("Error: Nearest commont ancestor of %s and %s\n" %
                (node1, node2))

    def hops_between(self, node1, node2):
        """
        return: hops between node1 and node2
        """
        def hops_rec(node, ancestor):
            if node == ancestor:
                return 0
            else:
                parent = self.parent_of(node)
                return hops_rec(parent, ancestor) + 1
                
        nca = self.nca_of(node1, node2)
        return hops_rec(node1, nca) + hops_rec(node2, nca)

    def print_tree(self, locations):
        """
        Print topology tree into graphviz format file
        """
        file = graph = locations.replace(":", "_")
        header = "digraph %s {\n" % graph
        footer = "}\n"
        fd = open(file + ".dot", "w")
        fd.write(header)
        switches = map(lambda s:self.map[s], 
                       self.get_switches(locations))
        # Start printing tree
        # Initial switch list
        for s in switches:
            src = s.getAttribute(TAG_ID)
            children = []
            for c in s.childNodes:
                if c.nodeName == TAG_HOST or \
                   c.nodeName == TAG_SWITCH:
                    children.append(c)
            for c in children:
                des = c.getAttribute(TAG_ID)
                fd.write("\t\"%s\"->\"%s\";\n" % (src, des))
        # End printing tree
        fd.write(footer)
        fd.close()
