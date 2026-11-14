-- Keep figure captions with their images and leave space around the group.
local function latex(block)
  return pandoc.write(pandoc.Pandoc({block}), "latex"):gsub("%s+$", "")
end

-- Cross-references. Figures and tables carry an identifier (#fig-… or #tbl-…) and
-- print their number as part of the caption ("图 3-1　…"). A reference is an
-- ordinary link whose text is that label: [图 3-1](#fig-architecture). The check
-- pass below fails the build if the text no longer matches the caption or the
-- target does not exist, so renumbering cannot leave stale references.
local targets = {}

local function caption_label(blocks)
  local text = pandoc.utils.stringify(blocks)
  return text:match("^(.-)\u{3000}") or text
end

local function register(identifier, label)
  if identifier ~= "" then targets[identifier] = label or true end
end

local check = {
  Header = function(header) register(header.identifier) end,
  Figure = function(figure)
    register(figure.identifier, caption_label(figure.caption.long))
  end,
  Table = function(tbl)
    register(tbl.identifier, caption_label(tbl.caption.long))
  end,
  Pandoc = function(doc)
    doc:walk({Link = function(link)
      local id = link.target:match("^#(.+)$")
      if not id then return nil end
      local label = targets[id]
      if label == nil then
        error("Link to unknown identifier #" .. id)
      end
      if type(label) == "string" and pandoc.utils.stringify(link.content) ~= label then
        error(string.format("Reference text '%s' does not match caption label '%s' of #%s",
          pandoc.utils.stringify(link.content), label, id))
      end
    end})
  end,
}

-- Place the group note above the bold, evenly spaced author names.
function Meta(meta)
  if not FORMAT:match("latex") or not meta.author then
    return nil
  end
  local function metadata_latex(value)
    local blocks = value
    if pandoc.utils.type(value) ~= "Blocks" then
      blocks = {pandoc.Plain(value)}
    end
    return pandoc.write(pandoc.Pandoc(blocks), "latex"):gsub("%s+$", "")
  end
  local authors = meta.author
  if pandoc.utils.type(authors) ~= "List" then authors = {authors} end
  local names = {}
  for _, author in ipairs(authors) do
    names[#names + 1] = "\\textbf{" .. metadata_latex(author) .. "}"
  end
  local rendered = ""
  if meta.affiliation then
    rendered = "{\\normalsize " .. metadata_latex(meta.affiliation) .. "}"
  end
  if meta.thanks then
    rendered = rendered .. "\\thanks{\\protect\\parbox[t]{\\dimexpr\\linewidth-1.8em\\relax}{"
      .. metadata_latex(meta.thanks) .. "}}"
  end
  if meta.affiliation then rendered = rendered .. "\\\\[0.3em]" end
  rendered = rendered .. table.concat(names, "\\hspace{1.5em}")
  meta.author = pandoc.MetaInlines({pandoc.RawInline("latex", rendered)})
  meta.thanks = nil
  return meta
end

function Figure(figure)
  if not FORMAT:match("latex") then return nil end
  if #figure.content ~= 1 or figure.content[1].t ~= "Plain"
      or #figure.content[1].content ~= 1 or figure.content[1].content[1].t ~= "Image" then
    error("Report figures must contain one standalone image")
  end
  figure.content[1].content[1].caption = {}
  local image = latex(figure.content[1])
  local caption = pandoc.write(pandoc.Pandoc(figure.caption.long), "latex")
  -- \phantomsection gives hyperref an anchor for \label without a counter.
  local anchor = ""
  if figure.identifier ~= "" then
    anchor = "\\phantomsection\\label{" .. figure.identifier .. "}"
  end
  return pandoc.RawBlock("latex", table.concat({
    "\\begin{center}",
    "\\begin{minipage}{\\linewidth}",
    anchor,
    "\\vspace*{8pt}",
    "\\centering",
    image .. "\\par\\medskip",
    "{\\small " .. caption .. "\\par}",
    "\\vspace{8pt}",
    "\\end{minipage}",
    "\\end{center}"
  }, "\n"))
end

function Table(table)
  if not FORMAT:match("latex") then return nil end
  -- Permit long PascalCase identifiers to wrap at word boundaries in cells.
  table = table:walk({Code = function(code)
    local rendered = pandoc.write(pandoc.Pandoc({pandoc.Plain({code})}), "latex")
    rendered = rendered:gsub("([a-z])([A-Z])", "%1\\allowbreak{}%2")
    return pandoc.RawInline("latex", rendered:gsub("%s+$", ""))
  end})
  return pandoc.RawBlock("latex", "\\Needspace{8\\baselineskip}\n" .. latex(table))
end

function Header(header)
  if not FORMAT:match("latex") then return nil end
  for _, class in ipairs(header.classes) do
    if class == "page-break" then
      return {pandoc.RawBlock("latex", "\\clearpage"), header}
    end
  end
end

-- Pandoc runs the filters in order: validate references on the unmodified
-- document, then rewrite it for LaTeX.
return {
  {Header = check.Header, Figure = check.Figure, Table = check.Table, Pandoc = check.Pandoc},
  {Meta = Meta, Figure = Figure, Table = Table, Header = Header},
}
