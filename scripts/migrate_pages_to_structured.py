#!/usr/bin/env python3
"""
Migrate pages from HTML content to structured_data.

This script:
1. Parses HTML content from pages (leadership, contacts, documents)
2. Extracts structured data
3. Updates structured_data field
4. Clears HTML content

Usage:
    python scripts/migrate_pages_to_structured.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from bs4 import BeautifulSoup
from sqlalchemy import select, update
from app.database import AsyncSessionLocal
from app.models import Page


async def migrate_leadership(db):
    """Migrate leadership pages from HTML to structured_data."""
    print("\n=== Migrating Leadership Pages ===")

    result = await db.execute(
        select(Page).where(Page.slug.in_(["basshylyk", "rukovodstvo"]))
    )
    pages = result.scalars().all()

    for page in pages:
        if not page.content:
            print(f"  {page.slug} ({page.language}): No content, skipping")
            continue

        soup = BeautifulSoup(page.content, 'html.parser')
        members = []

        for idx, card in enumerate(soup.find_all('div', class_='card'), 1):
            img = card.find('img')
            title = card.find('h4')
            position = card.find('p', class_='text-muted')
            bio = card.find('p', class_='card-text')

            if title:
                members.append({
                    'id': idx,
                    'name': title.get_text(strip=True) if title else None,
                    'position': position.get_text(strip=True) if position else None,
                    'photo': img.get('src') if img else None,
                    'bio': bio.get_text(strip=True) if bio else None
                })

        if members:
            # Update structured_data and clear HTML content
            page.structured_data = {'members': members}
            page.content = None
            page.content_text = f"Руководство QFL - {len(members)} человек"
            print(f"  {page.slug} ({page.language}): Extracted {len(members)} members")
        else:
            print(f"  {page.slug} ({page.language}): No members found in HTML")

    await db.commit()
    print("  Leadership migration completed!")


async def migrate_contacts(db):
    """Migrate contacts pages - extract contact info from HTML."""
    print("\n=== Migrating Contacts Pages ===")

    result = await db.execute(
        select(Page).where(Page.slug.in_(["baylanystar", "kontakty"]))
    )
    pages = result.scalars().all()

    for page in pages:
        if not page.content:
            print(f"  {page.slug} ({page.language}): No content, skipping")
            continue

        soup = BeautifulSoup(page.content, 'html.parser')

        # Extract contact info
        contacts = page.structured_data or {}

        # Extract emails from links
        emails = []
        for a in soup.find_all('a', href=True):
            if a['href'].startswith('mailto:'):
                emails.append(a['href'].replace('mailto:', ''))
        if emails:
            contacts['emails'] = emails

        # Extract phone numbers (pattern: +7 xxx xxx xx xx)
        import re
        text = soup.get_text()
        phones = re.findall(r'\+7[\s\d-]{10,15}', text)
        if phones:
            contacts['phones'] = [p.strip() for p in phones]

        # Extract contact person
        strong_tags = soup.find_all('strong')
        for strong in strong_tags:
            if 'вопрос' in strong.get_text().lower():
                next_text = strong.next_sibling
                if next_text:
                    # Try to extract name
                    name_match = re.search(r'([А-ЯӘӨҮІҢҒҚа-яәөүіңғқ]+\s+[А-ЯӘӨҮІҢҒҚа-яәөүіңғқ]+(?:\s+[А-ЯӘӨҮІҢҒҚа-яәөүіңғқ]+)?)', str(next_text))
                    if name_match:
                        contacts['contact_person'] = name_match.group(1).strip()

        if contacts:
            page.structured_data = contacts
            page.content = None
            page.content_text = "Контактная информация QFL"
            print(f"  {page.slug} ({page.language}): Updated contacts")
        else:
            print(f"  {page.slug} ({page.language}): No new data extracted")

    await db.commit()
    print("  Contacts migration completed!")


async def migrate_documents(db):
    """Migrate documents pages - extract document links from HTML."""
    print("\n=== Migrating Documents Pages ===")

    result = await db.execute(
        select(Page).where(Page.slug.in_(["kuzhattar", "dokumenty"]))
    )
    pages = result.scalars().all()

    for page in pages:
        if not page.content:
            print(f"  {page.slug} ({page.language}): No content, skipping")
            continue

        soup = BeautifulSoup(page.content, 'html.parser')
        documents = []

        # Extract document links
        for idx, link in enumerate(soup.find_all('a', class_='doc-link'), 1):
            href = link.get('href', '')
            title_elem = link.find('p', class_='doc-title')

            if href and title_elem:
                documents.append({
                    'id': idx,
                    'title': title_elem.get_text(strip=True),
                    'url': href if href.startswith('http') else f"https://kffleague.kz{href}",
                    'type': 'pdf' if '.pdf' in href.lower() else 'document'
                })

        if documents:
            page.structured_data = {'documents': documents}
            page.content = None
            page.content_text = f"Документы QFL - {len(documents)} файлов"
            print(f"  {page.slug} ({page.language}): Extracted {len(documents)} documents")
        else:
            print(f"  {page.slug} ({page.language}): No documents found in HTML")

    await db.commit()
    print("  Documents migration completed!")


async def main():
    print("=" * 60)
    print("Pages Migration: HTML → structured_data")
    print("=" * 60)

    async with AsyncSessionLocal() as db:
        await migrate_leadership(db)
        await migrate_contacts(db)
        await migrate_documents(db)

    print("\n" + "=" * 60)
    print("Migration completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
