package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const ProjectDefinitionSchema = 1

type ProjectDefinitionLayout string

const (
	ProjectDefinitionLegacy     ProjectDefinitionLayout = "legacy"
	ProjectDefinitionSplit      ProjectDefinitionLayout = "split"
	ProjectDefinitionMixed      ProjectDefinitionLayout = "mixed"
	ProjectDefinitionIncomplete ProjectDefinitionLayout = "incomplete"
)

type ProjectDefinition struct {
	Layout              ProjectDefinitionLayout
	Revision            string
	WorkflowPath        string
	ConfigPath          string
	LocalWorkflowPath   string
	LocalConfigPath     string
	LegacyKeys          []string
	LocalLegacyKeys     []string
	ConfiguredLocalKeys []string
	Migratable          bool
}

type ProjectDefinitionSources struct {
	WorkflowPath      string
	Workflow          []byte
	ConfigPath        string
	Config            []byte
	HasConfig         bool
	LocalWorkflowPath string
	LocalWorkflow     []byte
	HasLocalWorkflow  bool
	LocalConfigPath   string
	LocalConfig       []byte
	HasLocalConfig    bool
	AgentsPath        string
	Agents            []byte
	HasAgents         bool
}

type ProjectDefinitionError struct {
	Definition ProjectDefinition
	Err        error
}

func (e *ProjectDefinitionError) Error() string {
	if e == nil || e.Err == nil {
		return "invalid project definition"
	}
	return e.Err.Error()
}

func (e *ProjectDefinitionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func DefinitionPath(workflowPath string) string {
	return filepath.Join(filepath.Dir(workflowPath), "detent.yaml")
}

func LocalDefinitionPath(workflowPath string) string {
	return filepath.Join(filepath.Dir(workflowPath), "detent.local.yaml")
}

func LoadProjectDefinition(path string) (Workflow, error) {
	sources, err := readProjectDefinitionSources(path)
	if err != nil {
		return Workflow{}, err
	}
	return ParseProjectDefinition(sources)
}

func readProjectDefinitionSources(path string) (ProjectDefinitionSources, error) {
	workflowRaw, err := os.ReadFile(path)
	if err != nil {
		return ProjectDefinitionSources{}, &ProjectDefinitionError{
			Definition: projectDefinitionForPath(path),
			Err:        fmt.Errorf("read workflow file: %w", err),
		}
	}

	sources := ProjectDefinitionSources{
		WorkflowPath:      path,
		Workflow:          workflowRaw,
		ConfigPath:        DefinitionPath(path),
		LocalWorkflowPath: LocalWorkflowPath(path),
		LocalConfigPath:   LocalDefinitionPath(path),
	}
	if sources.Config, sources.HasConfig, err = readOptionalDefinitionFile(sources.ConfigPath); err != nil {
		return ProjectDefinitionSources{}, projectDefinitionReadError(path, "read project config", err)
	}
	if sources.LocalWorkflow, sources.HasLocalWorkflow, err = readOptionalDefinitionFile(sources.LocalWorkflowPath); err != nil {
		return ProjectDefinitionSources{}, projectDefinitionReadError(path, "read local workflow overlay", err)
	}
	if sources.LocalConfig, sources.HasLocalConfig, err = readOptionalDefinitionFile(sources.LocalConfigPath); err != nil {
		return ProjectDefinitionSources{}, projectDefinitionReadError(path, "read local project config", err)
	}
	sources.AgentsPath = filepath.Join(filepath.Dir(path), BacklogAdmissionEffortFileAgents)
	if sources.Agents, sources.HasAgents, err = readOptionalDefinitionFile(sources.AgentsPath); err != nil {
		return ProjectDefinitionSources{}, projectDefinitionReadError(path, "read agent guidance", err)
	}
	return sources, nil
}

func ParseProjectDefinition(sources ProjectDefinitionSources) (Workflow, error) {
	definition := projectDefinitionFromSources(sources)
	shared, err := splitProjectWorkflow(sources.Workflow)
	if err != nil {
		return Workflow{}, definitionError(definition, fmt.Errorf("parse %s: %w", displayDefinitionPath(sources.WorkflowPath, "WORKFLOW.md"), err))
	}
	local := projectWorkflowDocument{}
	if sources.HasLocalWorkflow {
		local, err = splitProjectWorkflow(sources.LocalWorkflow)
		if err != nil {
			return Workflow{}, definitionError(definition, fmt.Errorf("parse %s: %w", displayDefinitionPath(sources.LocalWorkflowPath, "WORKFLOW.local.md"), err))
		}
	}

	definition.LegacyKeys, err = projectDefinitionKeys(shared.frontmatter)
	if err != nil {
		return Workflow{}, definitionError(definition, fmt.Errorf("parse legacy workflow config: %w", err))
	}
	definition.LocalLegacyKeys, err = projectDefinitionKeys(local.frontmatter)
	if err != nil {
		return Workflow{}, definitionError(definition, fmt.Errorf("parse local legacy workflow config: %w", err))
	}
	definition.Layout, definition.Migratable = classifyProjectDefinition(sources, shared, local)

	switch definition.Layout {
	case ProjectDefinitionLegacy:
		workflow, parseErr := parseLegacyProjectDefinition(sources, shared, local)
		if parseErr != nil {
			return Workflow{}, definitionError(definition, parseErr)
		}
		workflow = attachProjectDefinitionAgents(workflow, sources)
		workflow.Definition = definition
		workflow.Definition.Revision = workflow.SourceHash
		if validationErr := ValidateWorkflowAdmission(workflow); validationErr != nil {
			return Workflow{}, definitionError(definition, validationErr)
		}
		return workflow, nil
	case ProjectDefinitionSplit:
		workflow, parseErr := parseSplitProjectDefinition(sources, shared, local)
		if parseErr != nil {
			return Workflow{}, definitionError(definition, parseErr)
		}
		workflow = attachProjectDefinitionAgents(workflow, sources)
		workflow.Definition = definition
		workflow.Definition.Revision = workflow.SourceHash
		if validationErr := ValidateWorkflowAdmission(workflow); validationErr != nil {
			return Workflow{}, definitionError(definition, validationErr)
		}
		return workflow, nil
	case ProjectDefinitionMixed:
		return Workflow{}, definitionError(definition, mixedProjectDefinitionError(sources, shared, local))
	default:
		return Workflow{}, definitionError(definition, fmt.Errorf(
			"incomplete project definition: %s is prompt-only and %s is missing",
			displayDefinitionPath(sources.WorkflowPath, "WORKFLOW.md"),
			displayDefinitionPath(sources.ConfigPath, "detent.yaml"),
		))
	}
}

func projectDefinitionForPath(path string) ProjectDefinition {
	return ProjectDefinition{
		Layout:            ProjectDefinitionIncomplete,
		WorkflowPath:      path,
		ConfigPath:        DefinitionPath(path),
		LocalWorkflowPath: LocalWorkflowPath(path),
		LocalConfigPath:   LocalDefinitionPath(path),
	}
}

func projectDefinitionFromSources(sources ProjectDefinitionSources) ProjectDefinition {
	definition := projectDefinitionForPath(sources.WorkflowPath)
	definition.Layout = initialProjectDefinitionLayout(sources)
	if strings.TrimSpace(sources.ConfigPath) != "" {
		definition.ConfigPath = sources.ConfigPath
	}
	if strings.TrimSpace(sources.LocalWorkflowPath) != "" {
		definition.LocalWorkflowPath = sources.LocalWorkflowPath
	}
	if strings.TrimSpace(sources.LocalConfigPath) != "" {
		definition.LocalConfigPath = sources.LocalConfigPath
	}
	return definition
}

func initialProjectDefinitionLayout(sources ProjectDefinitionSources) ProjectDefinitionLayout {
	sharedFrontmatter := hasProjectDefinitionFrontmatter(sources.Workflow)
	localFrontmatter := sources.HasLocalWorkflow && hasProjectDefinitionFrontmatter(sources.LocalWorkflow)
	if sources.HasConfig {
		if sharedFrontmatter || localFrontmatter {
			return ProjectDefinitionMixed
		}
		return ProjectDefinitionSplit
	}
	if sharedFrontmatter {
		return ProjectDefinitionLegacy
	}
	if sources.HasLocalConfig || localFrontmatter {
		return ProjectDefinitionMixed
	}
	return ProjectDefinitionIncomplete
}

func hasProjectDefinitionFrontmatter(raw []byte) bool {
	raw = bytes.TrimPrefix(raw, []byte{0xef, 0xbb, 0xbf})
	return bytes.HasPrefix(raw, []byte("---\n")) || bytes.HasPrefix(raw, []byte("---\r\n"))
}

func projectDefinitionReadError(path string, operation string, err error) error {
	return &ProjectDefinitionError{
		Definition: projectDefinitionForPath(path),
		Err:        fmt.Errorf("%s: %w", operation, err),
	}
}

func definitionError(definition ProjectDefinition, err error) error {
	return &ProjectDefinitionError{Definition: definition, Err: err}
}

func readOptionalDefinitionFile(path string) ([]byte, bool, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

type projectWorkflowDocument struct {
	frontmatter    []byte
	prompt         []byte
	hasFrontmatter bool
	lineEnding     string
}

func splitProjectWorkflow(raw []byte) (projectWorkflowDocument, error) {
	doc := projectWorkflowDocument{prompt: raw, lineEnding: "\n"}
	prefix := 0
	if bytes.HasPrefix(raw, []byte{0xef, 0xbb, 0xbf}) {
		prefix = 3
	}
	remaining := raw[prefix:]
	bodyOffset := prefix
	switch {
	case bytes.HasPrefix(remaining, []byte("---\r\n")):
		bodyOffset += len("---\r\n")
		doc.lineEnding = "\r\n"
	case bytes.HasPrefix(remaining, []byte("---\n")):
		bodyOffset += len("---\n")
	default:
		return doc, nil
	}

	for lineStart := bodyOffset; lineStart <= len(raw); {
		lineEnd := bytes.IndexByte(raw[lineStart:], '\n')
		next := len(raw)
		if lineEnd >= 0 {
			lineEnd += lineStart
			next = lineEnd + 1
		} else {
			lineEnd = len(raw)
		}
		line := bytes.TrimSuffix(raw[lineStart:lineEnd], []byte{'\r'})
		if bytes.Equal(line, []byte("---")) {
			doc.frontmatter = raw[bodyOffset:lineStart]
			doc.prompt = raw[next:]
			doc.hasFrontmatter = true
			return doc, nil
		}
		if next >= len(raw) {
			break
		}
		lineStart = next
	}
	return projectWorkflowDocument{}, errors.New("unterminated YAML frontmatter")
}

func classifyProjectDefinition(
	sources ProjectDefinitionSources,
	shared projectWorkflowDocument,
	local projectWorkflowDocument,
) (ProjectDefinitionLayout, bool) {
	sharedStructured := shared.hasFrontmatter && len(bytes.TrimSpace(shared.frontmatter)) > 0
	localStructured := local.hasFrontmatter && len(bytes.TrimSpace(local.frontmatter)) > 0
	if sources.HasConfig {
		if sharedStructured || localStructured {
			return ProjectDefinitionMixed, !sharedStructured && localStructured && !sources.HasLocalConfig
		}
		return ProjectDefinitionSplit, false
	}
	if shared.hasFrontmatter {
		if sources.HasLocalConfig {
			return ProjectDefinitionMixed, false
		}
		return ProjectDefinitionLegacy, true
	}
	if sources.HasLocalConfig || localStructured {
		return ProjectDefinitionMixed, false
	}
	return ProjectDefinitionIncomplete, false
}

func parseLegacyProjectDefinition(
	sources ProjectDefinitionSources,
	shared projectWorkflowDocument,
	local projectWorkflowDocument,
) (Workflow, error) {
	for _, source := range []struct {
		raw  []byte
		path string
	}{
		{legacyWorkflowBytes(shared), sources.WorkflowPath},
		{legacyWorkflowBytes(local), sources.LocalWorkflowPath},
	} {
		if source.path == sources.LocalWorkflowPath && !sources.HasLocalWorkflow {
			continue
		}
		if _, _, err := parseWorkflowDocument(source.raw); err != nil {
			return Workflow{}, fmt.Errorf("parse %s: %w", source.path, err)
		}
	}
	sharedRaw := legacyWorkflowBytes(shared)
	if !sources.HasLocalWorkflow {
		return ParseWorkflow(sharedRaw)
	}
	return ParseWorkflowOverlay(sharedRaw, legacyWorkflowBytes(local), sources.LocalWorkflowPath)
}

func legacyWorkflowBytes(doc projectWorkflowDocument) []byte {
	if !doc.hasFrontmatter {
		return doc.prompt
	}
	lineEnding := doc.lineEnding
	if lineEnding == "" {
		lineEnding = "\n"
	}
	out := make([]byte, 0, len(doc.frontmatter)+len(doc.prompt)+12)
	out = append(out, "---"...)
	out = append(out, lineEnding...)
	out = append(out, doc.frontmatter...)
	if len(doc.frontmatter) > 0 && !bytes.HasSuffix(doc.frontmatter, []byte("\n")) {
		out = append(out, lineEnding...)
	}
	out = append(out, "---"...)
	out = append(out, lineEnding...)
	out = append(out, doc.prompt...)
	return out
}

func parseSplitProjectDefinition(
	sources ProjectDefinitionSources,
	shared projectWorkflowDocument,
	local projectWorkflowDocument,
) (Workflow, error) {
	if shared.hasFrontmatter && len(bytes.TrimSpace(shared.frontmatter)) > 0 {
		return Workflow{}, errors.New("mixed project definition: Detent configuration exists in both detent.yaml and WORKFLOW.md")
	}
	sharedRoot, err := parseSchemaConfig(sources.Config, sources.ConfigPath)
	if err != nil {
		return Workflow{}, err
	}
	var localRoot *yaml.Node
	if sources.HasLocalConfig {
		localRoot, err = parseSchemaConfig(sources.LocalConfig, sources.LocalConfigPath)
		if err != nil {
			return Workflow{}, err
		}
		mergeWorkflowMappings(sharedRoot, localRoot)
	}
	cfg, err := decodeWorkflowConfig(sharedRoot)
	if err != nil {
		return Workflow{}, fmt.Errorf("decode project config: %w", err)
	}

	sharedPrompt := normalizeProjectDefinitionPrompt(shared.prompt)
	prompt := string(sharedPrompt)
	if sources.HasLocalWorkflow {
		prompt = mergeWorkflowPrompts(sharedPrompt, normalizeProjectDefinitionPrompt(local.prompt))
	}
	hash := projectDefinitionHash(sources, false)
	workflow := Workflow{
		Config:       cfg,
		Prompt:       prompt,
		SharedPrompt: string(sharedPrompt),
		SourceHash:   hash,
	}
	if sources.HasLocalWorkflow || sources.HasLocalConfig {
		path := sources.LocalConfigPath
		if !sources.HasLocalConfig {
			path = sources.LocalWorkflowPath
		}
		workflow.Overlay = WorkflowOverlay{
			Path:           path,
			OverriddenKeys: sortedConfiguredFieldPaths(localRoot),
		}
	}
	return workflow, nil
}

func normalizeProjectDefinitionPrompt(prompt []byte) []byte {
	normalized := bytes.ReplaceAll(prompt, []byte("\r\n"), []byte("\n"))
	return bytes.TrimPrefix(normalized, []byte{0xef, 0xbb, 0xbf})
}

func parseSchemaConfig(raw []byte, path string) (*yaml.Node, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", displayDefinitionPath(path, "detent.yaml"), err)
	}
	root, err := documentRoot(&doc)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", displayDefinitionPath(path, "detent.yaml"), err)
	}
	if root.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("parse %s: project config must be a YAML mapping", displayDefinitionPath(path, "detent.yaml"))
	}
	index := mappingKeyIndex(root, "schema")
	if index < 0 {
		return nil, fmt.Errorf("parse %s: schema is required", displayDefinitionPath(path, "detent.yaml"))
	}
	var schema int
	if err := root.Content[index+1].Decode(&schema); err != nil {
		return nil, fmt.Errorf("parse %s: schema must be an integer", displayDefinitionPath(path, "detent.yaml"))
	}
	if schema != ProjectDefinitionSchema {
		return nil, fmt.Errorf(
			"parse %s: unsupported schema version %d; supported version is %d",
			displayDefinitionPath(path, "detent.yaml"),
			schema,
			ProjectDefinitionSchema,
		)
	}
	if err := validateSandboxPolicyNodes(root); err != nil {
		return nil, fmt.Errorf("parse %s: %w", displayDefinitionPath(path, "detent.yaml"), err)
	}
	root.Content = append(root.Content[:index], root.Content[index+2:]...)
	return root, nil
}

func projectDefinitionKeys(frontmatter []byte) ([]string, error) {
	if len(bytes.TrimSpace(frontmatter)) == 0 {
		return nil, nil
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(frontmatter, &doc); err != nil {
		return nil, err
	}
	root, err := documentRoot(&doc)
	if err != nil {
		return nil, err
	}
	if root.Kind != yaml.MappingNode {
		return nil, errors.New("workflow frontmatter must be a mapping")
	}
	keys := make([]string, 0, len(root.Content)/2)
	for index := 0; index+1 < len(root.Content); index += 2 {
		keys = append(keys, root.Content[index].Value)
	}
	sort.Strings(keys)
	return keys, nil
}

func mixedProjectDefinitionError(
	sources ProjectDefinitionSources,
	shared projectWorkflowDocument,
	local projectWorkflowDocument,
) error {
	var conflicts []string
	if sources.HasConfig && shared.hasFrontmatter && len(bytes.TrimSpace(shared.frontmatter)) > 0 {
		conflicts = append(conflicts, displayDefinitionPath(sources.ConfigPath, "detent.yaml")+" and "+displayDefinitionPath(sources.WorkflowPath, "WORKFLOW.md"))
	}
	if local.hasFrontmatter && len(bytes.TrimSpace(local.frontmatter)) > 0 {
		conflicts = append(conflicts, displayDefinitionPath(sources.LocalWorkflowPath, "WORKFLOW.local.md")+" structured frontmatter")
	}
	if sources.HasLocalConfig && !sources.HasConfig {
		conflicts = append(conflicts, displayDefinitionPath(sources.LocalConfigPath, "detent.local.yaml")+" without detent.yaml")
	}
	if len(conflicts) == 0 {
		conflicts = append(conflicts, "multiple project-definition sources")
	}
	return errors.New("mixed project definition has ambiguous authority: " + strings.Join(conflicts, "; "))
}

func attachProjectDefinitionAgents(workflow Workflow, sources ProjectDefinitionSources) Workflow {
	admission := workflow.Config.BacklogAdmission
	admission.Normalize()
	if !admission.RequireEffort {
		return workflow
	}
	if sources.HasAgents && (admission.EffortFile == BacklogAdmissionEffortFileAgents ||
		!admissionHeadingExists(workflow.SharedPrompt, admission.EffortSection)) {
		workflow.AgentsPrompt = string(normalizeProjectDefinitionPrompt(sources.Agents))
	}
	if admission.EffortFile == BacklogAdmissionEffortFileAgents {
		workflow.SourceHash = projectDefinitionHash(sources, true)
	}
	return workflow
}

func projectDefinitionHash(sources ProjectDefinitionSources, includeAgents bool) string {
	hash := sha256.New()
	for _, source := range []struct {
		name    string
		raw     []byte
		present bool
	}{
		{name: "WORKFLOW.md", raw: sources.Workflow, present: true},
		{name: "detent.yaml", raw: sources.Config, present: sources.HasConfig},
		{name: "WORKFLOW.local.md", raw: sources.LocalWorkflow, present: sources.HasLocalWorkflow},
		{name: "detent.local.yaml", raw: sources.LocalConfig, present: sources.HasLocalConfig},
		{name: BacklogAdmissionEffortFileAgents, raw: sources.Agents, present: includeAgents && sources.HasAgents},
	} {
		if !source.present {
			continue
		}
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(source.name))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(source.raw)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func displayDefinitionPath(path string, fallback string) string {
	if strings.TrimSpace(path) == "" {
		return fallback
	}
	return path
}
