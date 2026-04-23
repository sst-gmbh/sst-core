// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst_test

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"git.semanticstep.net/x/sst/sst"
	"git.semanticstep.net/x/sst/vocabularies/countrycodes"
	_ "git.semanticstep.net/x/sst/vocabularies/dict"
	"git.semanticstep.net/x/sst/vocabularies/lci"
	"git.semanticstep.net/x/sst/vocabularies/rdf"
	"git.semanticstep.net/x/sst/vocabularies/rdfs"
	"git.semanticstep.net/x/sst/vocabularies/rep"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to write Stage to TriG file
func writeStageToTriGFile(stage sst.Stage, fileName string) error {
	f, err := os.Create(fileName + ".trig")
	if err != nil {
		return err
	}
	defer f.Close()

	return stage.RdfWrite(f, sst.RdfFormatTriG)
}

// Helper function to read TriG file and return Stage
func readTriGFile(fileName string) (sst.Stage, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return sst.RdfRead(bufio.NewReader(file), sst.RdfFormatTriG, sst.StrictHandler, sst.DefaultTriplexMode)
}

// Test_StageRdfWriteTriG_SingleGraph tests writing a single named graph to TriG format
func Test_StageRdfWriteTriG_SingleGraph(t *testing.T) {
	testName := filepath.Join("./testdata/" + t.Name())

	t.Run("write_single_graph", func(t *testing.T) {
		stage := sst.OpenStage(sst.DefaultTriplexMode)

		ngID := uuid.MustParse("c1efcf54-3e8e-4cc7-a7d1-82a9f613a361")
		ng := stage.CreateNamedGraph(sst.IRI(ngID.URN()))

		// Create some data
		jane := ng.CreateIRINode("Jane", lci.Person)
		organization := ng.CreateIRINode("ECT", lci.Organization)
		organization.AddStatement(rdfs.Label, sst.String("ECT"))

		workFor := ng.CreateIRINode("workFor", rdf.Property)
		workFor.AddStatement(rdfs.Domain, lci.Person)
		workFor.AddStatement(rdfs.Range, lci.Organization)

		jane.AddStatement(workFor, organization)

		// Write stage to TriG
		err := writeStageToTriGFile(stage, testName)
		require.NoError(t, err)

		// Verify file was created
		_, err = os.Stat(testName + ".trig")
		assert.NoError(t, err)
	})
}

// Test_StageRdfWriteTriG_MultipleGraphs tests writing multiple named graphs to TriG format
func Test_StageRdfWriteTriG_MultipleGraphs(t *testing.T) {
	testName := filepath.Join("./testdata/" + t.Name())

	t.Run("write_multiple_graphs", func(t *testing.T) {
		stage := sst.OpenStage(sst.DefaultTriplexMode)

		// Create first named graph - People
		ngID1 := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1")
		ng1 := stage.CreateNamedGraph(sst.IRI(ngID1.URN()))

		jane := ng1.CreateIRINode("Jane", lci.Person)
		john := ng1.CreateIRINode("John", lci.Person)
		jane.AddStatement(rdfs.Label, sst.String("Jane Doe"))
		john.AddStatement(rdfs.Label, sst.String("John Doe"))

		// Create second named graph - Organizations
		ngID2 := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb2")
		ng2 := stage.CreateNamedGraph(sst.IRI(ngID2.URN()))

		org1 := ng2.CreateIRINode("CompanyA", lci.Organization)
		org2 := ng2.CreateIRINode("CompanyB", lci.Organization)
		org1.AddStatement(rdfs.Label, sst.String("Company A"))
		org2.AddStatement(rdfs.Label, sst.String("Company B"))

		// Create third named graph - Relationships
		ngID3 := uuid.MustParse("cccccccc-cccc-cccc-cccc-ccccccccccc3")
		ng3 := stage.CreateNamedGraph(sst.IRI(ngID3.URN()))

		workFor := ng3.CreateIRINode("workFor", rdf.Property)
		workFor.AddStatement(rdfs.Domain, lci.Person)
		workFor.AddStatement(rdfs.Range, lci.Organization)

		// Write stage to TriG
		err := writeStageToTriGFile(stage, testName)
		require.NoError(t, err)

		// Verify file was created and read content
		content, err := os.ReadFile(testName + ".trig")
		require.NoError(t, err)

		trigStr := string(content)
		// Verify all three graph IRIs appear in output
		assert.Contains(t, trigStr, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1")
		assert.Contains(t, trigStr, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb2")
		assert.Contains(t, trigStr, "cccccccc-cccc-cccc-cccc-ccccccccccc3")

		// Verify graph block structure
		assert.Contains(t, trigStr, "{")
		assert.Contains(t, trigStr, "}")

		// Verify prefixes
		assert.Contains(t, trigStr, "@prefix")
		assert.Contains(t, trigStr, "rdf:")
		assert.Contains(t, trigStr, "owl:")
	})
}

// Test_StageRdfWriteTriG_WithBlankNodes tests TriG output with blank nodes
func Test_StageRdfWriteTriG_WithBlankNodes(t *testing.T) {
	testName := filepath.Join("./testdata/" + t.Name())

	t.Run("write_with_blank_nodes", func(t *testing.T) {
		stage := sst.OpenStage(sst.DefaultTriplexMode)

		ngID := uuid.MustParse("dddddddd-dddd-dddd-dddd-ddddddddddd4")
		ng := stage.CreateNamedGraph(sst.IRI(ngID.URN()))

		jane := ng.CreateIRINode("Jane", lci.Person)

		// Create blank node for organization
		blankOrganization := ng.CreateBlankNode(lci.Organization)
		blankOrganization.AddStatement(rdfs.Label, sst.String("Anonymous Org"))

		workFor := ng.CreateIRINode("workFor", rdf.Property)
		workFor.AddStatement(rdfs.Domain, lci.Person)
		workFor.AddStatement(rdfs.Range, lci.Organization)

		jane.AddStatement(workFor, blankOrganization)

		// Write stage to TriG
		err := writeStageToTriGFile(stage, testName)
		require.NoError(t, err)

		// Read and verify content
		content, err := os.ReadFile(testName + ".trig")
		require.NoError(t, err)

		trigStr := string(content)
		// Verify blank node notation (inline blank nodes use [ ] syntax in Turtle/TriG)
		assert.Contains(t, trigStr, "[")
		assert.Contains(t, trigStr, "]")
		assert.Contains(t, trigStr, "Anonymous Org")
	})
}

// Test_StageRdfWriteTriG_WithCollections tests TriG output with RDF collections
func Test_StageRdfWriteTriG_WithCollections(t *testing.T) {
	testName := filepath.Join("./testdata/" + t.Name())

	t.Run("write_with_collections", func(t *testing.T) {
		stage := sst.OpenStage(sst.DefaultTriplexMode)

		ngID := uuid.MustParse("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeee5")
		ng := stage.CreateNamedGraph(sst.IRI(ngID.URN()))

		john := ng.CreateIRINode("John", lci.Person)
		jane := ng.CreateIRINode("Jane", lci.Person)
		adam := ng.CreateIRINode("Adam", lci.Person)

		// Create collection of friends
		friends := ng.CreateCollection(jane, adam)
		hasFriend := ng.CreateIRINode("hasFriend", rdf.Property)
		john.AddStatement(hasFriend, friends)

		// Write stage to TriG
		err := writeStageToTriGFile(stage, testName)
		require.NoError(t, err)

		// Read and verify content
		content, err := os.ReadFile(testName + ".trig")
		require.NoError(t, err)

		trigStr := string(content)
		// Verify collection notation (parentheses)
		assert.Contains(t, trigStr, "(")
		assert.Contains(t, trigStr, ")")
	})
}

// Test_StageRdfWriteTriG_WithLiteralCollections tests TriG output with literal collections
func Test_StageRdfWriteTriG_WithLiteralCollections(t *testing.T) {
	testName := filepath.Join("./testdata/" + t.Name())

	t.Run("write_with_literal_collections", func(t *testing.T) {
		stage := sst.OpenStage(sst.DefaultTriplexMode)

		ngID := uuid.MustParse("ffffffff-ffff-ffff-ffff-fffffffffff6")
		ng := stage.CreateNamedGraph(sst.IRI(ngID.URN()))

		white := ng.CreateIRINode("white", rep.ColourRGB)
		lc1 := sst.NewLiteralCollection(sst.Integer(255), sst.Integer(255), sst.Integer(255))
		white.AddStatement(rep.Rgb, lc1)

		// Write stage to TriG
		err := writeStageToTriGFile(stage, testName)
		require.NoError(t, err)

		// Read and verify content
		content, err := os.ReadFile(testName + ".trig")
		require.NoError(t, err)

		trigStr := string(content)
		// Verify literal values
		assert.Contains(t, trigStr, "255")
	})
}

// Test_StageRdfWriteTriG_GraphWithImports tests TriG output with imported graphs
func Test_StageRdfWriteTriG_GraphWithImports(t *testing.T) {
	testName := filepath.Join("./testdata/" + t.Name())

	t.Run("write_with_imports", func(t *testing.T) {
		stage := sst.OpenStage(sst.DefaultTriplexMode)

		// Create base vocabulary graph
		ngID1 := uuid.MustParse("11111111-1111-1111-1111-111111111111")
		ng1 := stage.CreateNamedGraph(sst.IRI(ngID1.URN()))

		workFor := ng1.CreateIRINode("workFor", rdf.Property)
		workFor.AddStatement(rdfs.Domain, lci.Person)
		workFor.AddStatement(rdfs.Range, lci.Organization)

		// Create data graph that imports vocabulary
		ngID2 := uuid.MustParse("22222222-2222-2222-2222-222222222222")
		ng2 := stage.CreateNamedGraph(sst.IRI(ngID2.URN()))
		ng2.AddImport(ng1)

		jane := ng2.CreateIRINode("Jane", lci.Person)
		org := ng2.CreateIRINode("ECT", lci.Organization)
		jane.AddStatement(workFor, org)

		// Write stage to TriG
		err := writeStageToTriGFile(stage, testName)
		require.NoError(t, err)

		// Read and verify content
		content, err := os.ReadFile(testName + ".trig")
		require.NoError(t, err)

		trigStr := string(content)
		// Verify both graphs are present
		assert.Contains(t, trigStr, "11111111-1111-1111-1111-111111111111")
		assert.Contains(t, trigStr, "22222222-2222-2222-2222-222222222222")
	})
}

// Test_StageRdfWriteTriG_ComplexScenario tests a complex scenario with multiple features
func Test_StageRdfWriteTriG_ComplexScenario(t *testing.T) {
	testName := filepath.Join("./testdata/" + t.Name())

	t.Run("write_complex_scenario", func(t *testing.T) {
		stage := sst.OpenStage(sst.DefaultTriplexMode)

		// Graph 1: People and their basic info
		ngID1 := uuid.MustParse("aaaaaaaa-1111-1111-1111-111111111111")
		ng1 := stage.CreateNamedGraph(sst.IRI(ngID1.URN()))

		john := ng1.CreateIRINode("John", lci.Person)
		jane := ng1.CreateIRINode("Jane", lci.Person)
		adam := ng1.CreateIRINode("Adam", lci.Person)

		john.AddStatement(rdfs.Label, sst.String("John Doe"))
		jane.AddStatement(rdfs.Label, sst.String("Jane Doe"))
		adam.AddStatement(rdfs.Label, sst.String("Adam Smith"))

		// Graph 2: Organizations
		ngID2 := uuid.MustParse("bbbbbbbb-2222-2222-2222-222222222222")
		ng2 := stage.CreateNamedGraph(sst.IRI(ngID2.URN()))

		org1 := ng2.CreateIRINode("CompanyA", lci.Organization)
		org2 := ng2.CreateIRINode("CompanyB", lci.Organization)
		blankOrg := ng2.CreateBlankNode(lci.Organization)

		org1.AddStatement(rdfs.Label, sst.String("Company A"))
		org2.AddStatement(rdfs.Label, sst.String("Company B"))
		blankOrg.AddStatement(rdfs.Label, sst.String("Stealth Startup"))

		// Graph 3: Relationships using collections
		ngID3 := uuid.MustParse("cccccccc-3333-3333-3333-333333333333")
		ng3 := stage.CreateNamedGraph(sst.IRI(ngID3.URN()))

		worksAt := ng3.CreateIRINode("worksAt", rdf.Property)
		hasFriends := ng3.CreateIRINode("hasFriends", rdf.Property)
		hasSkill := ng3.CreateIRINode("hasSkill", rdf.Property)

		// John works at CompanyA and has friends Jane and Adam
		johnRef := ng3.CreateIRINode("John")
		companyARef := ng3.CreateIRINode("CompanyA")
		friends := ng3.CreateCollection(jane, adam)

		johnRef.AddStatement(worksAt, companyARef)
		johnRef.AddStatement(hasFriends, friends)

		// Skills as literal collection
		skills := sst.NewLiteralCollection(sst.String("Go"), sst.String("Python"), sst.String("RDF"))
		johnRef.AddStatement(hasSkill, skills)

		// Graph 4: Geographic data with referenced vocab
		ngID4 := uuid.MustParse("dddddddd-4444-4444-4444-444444444444")
		ng4 := stage.CreateNamedGraph(sst.IRI(ngID4.URN()))

		johnGeo := ng4.CreateIRINode("John")
		janeGeo := ng4.CreateIRINode("Jane")

		johnGeo.AddStatement(lci.PartOf, countrycodes.Cn)
		janeGeo.AddStatement(lci.PartOf, countrycodes.De)

		// Write stage to TriG
		err := writeStageToTriGFile(stage, testName)
		require.NoError(t, err)

		// Read and verify content
		content, err := os.ReadFile(testName + ".trig")
		require.NoError(t, err)

		trigStr := string(content)

		// Verify all 4 graphs are present
		assert.Contains(t, trigStr, "aaaaaaaa-1111-1111-1111-111111111111")
		assert.Contains(t, trigStr, "bbbbbbbb-2222-2222-2222-222222222222")
		assert.Contains(t, trigStr, "cccccccc-3333-3333-3333-333333333333")
		assert.Contains(t, trigStr, "dddddddd-4444-4444-4444-444444444444")

		// Verify graph blocks
		assert.Contains(t, trigStr, "{")
		assert.Contains(t, trigStr, "}")

		// Verify data
		assert.Contains(t, trigStr, "John Doe")
		assert.Contains(t, trigStr, "Jane Doe")
		assert.Contains(t, trigStr, "Adam Smith")
		assert.Contains(t, trigStr, "Company A")
		assert.Contains(t, trigStr, "Stealth Startup")
	})
}

// Test_StageRdfWriteTriG_EmptyStage tests writing an empty stage
func Test_StageRdfWriteTriG_EmptyStage(t *testing.T) {
	testName := filepath.Join("./testdata/" + t.Name())

	t.Run("write_empty_stage", func(t *testing.T) {
		stage := sst.OpenStage(sst.DefaultTriplexMode)

		// Write empty stage to TriG - should handle gracefully
		err := writeStageToTriGFile(stage, testName)
		require.NoError(t, err)

		// Read content
		content, err := os.ReadFile(testName + ".trig")
		require.NoError(t, err)

		// Empty stage produces empty output (no graphs to write)
		trigStr := string(content)
		// Empty output is acceptable for empty stage
		_ = trigStr
	})
}

// Test_StageRdfWriteTriG_EmptyNamedGraph tests writing a stage with empty named graphs
func Test_StageRdfWriteTriG_EmptyNamedGraph(t *testing.T) {
	testName := filepath.Join("./testdata/" + t.Name())

	t.Run("write_empty_named_graph", func(t *testing.T) {
		stage := sst.OpenStage(sst.DefaultTriplexMode)

		// Create a named graph but don't add any nodes
		ngID := uuid.MustParse("99999999-9999-9999-9999-999999999999")
		_ = stage.CreateNamedGraph(sst.IRI(ngID.URN()))

		// Write stage to TriG
		err := writeStageToTriGFile(stage, testName)
		require.NoError(t, err)

		// Read content
		content, err := os.ReadFile(testName + ".trig")
		require.NoError(t, err)

		trigStr := string(content)
		// Should contain the graph IRI and empty block
		assert.Contains(t, trigStr, "99999999-9999-9999-9999-999999999999")
		assert.Contains(t, trigStr, "{")
		assert.Contains(t, trigStr, "}")
	})
}

// Test_NamedGraphRdfWriteTurtleVsStageRdfWriteTriG compares Turtle vs TriG output
func Test_NamedGraphRdfWriteTurtleVsStageRdfWriteTriG(t *testing.T) {
	testName := filepath.Join("./testdata/" + t.Name())

	t.Run("compare_turtle_vs_trig", func(t *testing.T) {
		stage := sst.OpenStage(sst.DefaultTriplexMode)

		ngID := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
		ng := stage.CreateNamedGraph(sst.IRI(ngID.URN()))

		jane := ng.CreateIRINode("Jane", lci.Person)
		jane.AddStatement(rdfs.Label, sst.String("Jane Doe"))

		// Write single graph using NamedGraph.RdfWrite (Turtle)
		turtleFile := testName + "_single.ttl"
		f, err := os.Create(turtleFile)
		require.NoError(t, err)
		err = ng.RdfWrite(f, sst.RdfFormatTurtle)
		f.Close()
		require.NoError(t, err)

		// Write using Stage.RdfWrite (TriG)
		trigFile := testName + "_single.trig"
		f2, err := os.Create(trigFile)
		require.NoError(t, err)
		err = stage.RdfWrite(f2, sst.RdfFormatTriG)
		f2.Close()
		require.NoError(t, err)

		// Read both files
		turtleContent, err := os.ReadFile(turtleFile)
		require.NoError(t, err)

		trigContent, err := os.ReadFile(trigFile)
		require.NoError(t, err)

		turtleStr := string(turtleContent)
		trigStr := string(trigContent)

		// Both should contain the data
		assert.Contains(t, turtleStr, "Jane Doe")
		assert.Contains(t, trigStr, "Jane Doe")

		// TriG should have graph blocks, Turtle should not
		assert.Contains(t, trigStr, "{")
		assert.Contains(t, trigStr, "}")
		assert.NotContains(t, turtleStr, "{")
		assert.NotContains(t, turtleStr, "}")
	})
}

// Test_StageRdfWriteTriG_MemoryBuffer tests writing to memory buffer
func Test_StageRdfWriteTriG_MemoryBuffer(t *testing.T) {
	t.Run("write_to_memory", func(t *testing.T) {
		stage := sst.OpenStage(sst.DefaultTriplexMode)

		ngID1 := uuid.MustParse("11111111-1111-1111-1111-111111111111")
		ng1 := stage.CreateNamedGraph(sst.IRI(ngID1.URN()))
		ng1.CreateIRINode("Node1", lci.Person)

		ngID2 := uuid.MustParse("22222222-2222-2222-2222-222222222222")
		ng2 := stage.CreateNamedGraph(sst.IRI(ngID2.URN()))
		ng2.CreateIRINode("Node2", lci.Organization)

		// Write to memory buffer
		var buf bytes.Buffer
		err := stage.RdfWrite(&buf, sst.RdfFormatTriG)
		require.NoError(t, err)

		trigStr := buf.String()

		// Verify content
		assert.Contains(t, trigStr, "11111111-1111-1111-1111-111111111111")
		assert.Contains(t, trigStr, "22222222-2222-2222-2222-222222222222")
		assert.Contains(t, trigStr, "{")
		assert.Contains(t, trigStr, "}")
		assert.Contains(t, trigStr, "@prefix")
	})
}

// Test_TriGFormatRoundTrip tests that TriG can be written and read back
func Test_TriGFormatRoundTrip(t *testing.T) {
	testName := filepath.Join("./testdata/" + t.Name())

	t.Run("round_trip_single_graph", func(t *testing.T) {
		// Create initial stage
		stage1 := sst.OpenStage(sst.DefaultTriplexMode)

		ngID := uuid.MustParse("aaaaaaaa-1111-1111-1111-111111111111")
		ng := stage1.CreateNamedGraph(sst.IRI(ngID.URN()))

		jane := ng.CreateIRINode("Jane", lci.Person)
		jane.AddStatement(rdfs.Label, sst.String("Jane Doe"))

		// Write to TriG file
		err := writeStageToTriGFile(stage1, testName+"_single")
		require.NoError(t, err)

		// Read back from TriG file
		stage2, err := readTriGFile(testName + "_single.trig")
		require.NoError(t, err)
		defer stage2.Close()

		// Verify data was preserved
		graphs := stage2.NamedGraphs()
		require.Len(t, graphs, 1)

		// Verify the graph IRI matches
		assert.Equal(t, "urn:uuid:aaaaaaaa-1111-1111-1111-111111111111", string(graphs[0].IRI()))
	})

	t.Run("round_trip_multiple_graphs", func(t *testing.T) {
		// Create initial stage with multiple graphs
		stage1 := sst.OpenStage(sst.DefaultTriplexMode)

		ngID1 := uuid.MustParse("bbbbbbbb-1111-1111-1111-111111111111")
		ng1 := stage1.CreateNamedGraph(sst.IRI(ngID1.URN()))
		jane := ng1.CreateIRINode("Jane", lci.Person)
		jane.AddStatement(rdfs.Label, sst.String("Jane Doe"))

		ngID2 := uuid.MustParse("bbbbbbbb-2222-2222-2222-222222222222")
		ng2 := stage1.CreateNamedGraph(sst.IRI(ngID2.URN()))
		org := ng2.CreateIRINode("CompanyA", lci.Organization)
		org.AddStatement(rdfs.Label, sst.String("Company A"))

		// Write to TriG file
		err := writeStageToTriGFile(stage1, testName+"_multi")
		require.NoError(t, err)

		// Read back from TriG file
		stage2, err := readTriGFile(testName + "_multi.trig")
		require.NoError(t, err)
		defer stage2.Close()

		// Verify data was preserved
		graphs := stage2.NamedGraphs()
		require.Len(t, graphs, 2)

		// Verify graph IRIs
		graphIRIs := make([]string, len(graphs))
		for i, g := range graphs {
			graphIRIs[i] = string(g.IRI())
		}
		assert.Contains(t, graphIRIs, "urn:uuid:bbbbbbbb-1111-1111-1111-111111111111")
		assert.Contains(t, graphIRIs, "urn:uuid:bbbbbbbb-2222-2222-2222-222222222222")
	})

	t.Run("round_trip_complex", func(t *testing.T) {
		// Create initial stage with complex data
		stage1 := sst.OpenStage(sst.DefaultTriplexMode)

		ngID1 := uuid.MustParse("cccccccc-1111-1111-1111-111111111111")
		ng1 := stage1.CreateNamedGraph(sst.IRI(ngID1.URN()))
		john := ng1.CreateIRINode("John", lci.Person)
		jane := ng1.CreateIRINode("Jane", lci.Person)
		john.AddStatement(rdfs.Label, sst.String("John Doe"))
		jane.AddStatement(rdfs.Label, sst.String("Jane Doe"))

		ngID2 := uuid.MustParse("cccccccc-2222-2222-2222-222222222222")
		ng2 := stage1.CreateNamedGraph(sst.IRI(ngID2.URN()))
		org := ng2.CreateIRINode("CompanyA", lci.Organization)
		org.AddStatement(rdfs.Label, sst.String("Company A"))

		// Write to TriG file
		err := writeStageToTriGFile(stage1, testName+"_complex")
		require.NoError(t, err)

		// Read back from TriG file
		stage2, err := readTriGFile(testName + "_complex.trig")
		require.NoError(t, err)
		defer stage2.Close()

		// Verify data was preserved
		graphs := stage2.NamedGraphs()
		require.Len(t, graphs, 2)
	})
}
